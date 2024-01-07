from fastapi import FastAPI, HTTPException, Depends, APIRouter
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Session
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime

app = FastAPI()

DATABASE_URL = "postgresql://postgres:150774@localhost:5432/employee_task"

Base = declarative_base()


class Employee(Base):
    __tablename__ = "employees"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    position = Column(String)
    tasks = relationship("Task", back_populates="executor")


class Task(Base):
    __tablename__ = "tasks"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    parent_task_id = Column(Integer, ForeignKey('tasks.id'), nullable=True)
    executor_id = Column(Integer, ForeignKey('employees.id'))
    deadline = Column(DateTime)
    status = Column(String)
    executor = relationship("Employee", back_populates="tasks")


Engine = create_engine(DATABASE_URL)
Base.metadata.create_all(bind=Engine)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=Engine)

Base = declarative_base()
engine = create_engine(DATABASE_URL)

# Новый роутер для очистки
router = APIRouter()


# Зависимость для получения сессии базы данных
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.delete("/cleanup")
def cleanup_tasks(db: Session = Depends(get_db)):
    # Удаление задач с определенным статусом (например, "completed")
    db.query(Task).filter(Task.status == "completed").delete()

    # Удаление всех связанных задач перед удалением сотрудников
    db.query(Task).delete()

    # Удаление всех сотрудников
    db.query(Employee).delete()

    db.commit()
    return {"message": "Очистка выполнена"}


app.include_router(router)


class EmployeeModel(BaseModel):
    name: str
    position: str


class TaskModel(BaseModel):
    __tablename__ = "tasks"

    name: str
    parent_task_id: Optional[int] = None
    executor_id: Optional[int] = None
    deadline: datetime
    status: str

    executor_id = Column(Integer, nullable=True)


class AssignedTask(BaseModel):
    employee_name: str
    task_name: str


# Операции CRUD для сотрудников
@app.post("/employees/", response_model=EmployeeModel)
def create_employee(employee: EmployeeModel, db: Session = Depends(get_db)):
    db_employee = Employee(**employee.dict())
    db.add(db_employee)
    db.commit()
    db.refresh(db_employee)
    return db_employee


@app.get("/employees/", response_model=List[EmployeeModel])
def get_employees(db: Session = Depends(get_db)):
    return db.query(Employee).all()


@app.delete("/employees/{employee_id}")
def delete_employee(employee_id: int, db: Session = Depends(get_db)):
    db.query(Employee).filter(Employee.id == employee_id).delete()
    db.commit()
    return {"message": "Сотрудник удален"}


# Операции CRUD для задач
@app.post("/tasks/", response_model=TaskModel)
def create_task(task: TaskModel, db: Session = Depends(get_db)):
    db_task = Task(**task.dict())
    db.add(db_task)
    db.commit()
    db.refresh(db_task)
    return db_task


@app.get("/tasks/", response_model=List[TaskModel])
def get_tasks(db: Session = Depends(get_db)):
    return db.query(Task).all()


@app.delete("/tasks/{task_id}")
def delete_task(task_id: int, db: Session = Depends(get_db)):
    db.query(Task).filter(Task.id == task_id).delete()
    db.commit()
    return {"message": "Задача удалена"}


# Endpoint для "Важных задач" и "Выполняемых задач"
@app.get("/important_tasks", response_model=List[AssignedTask])
def important_tasks(db: Session = Depends(get_db)):
    important_tasks = db.query(Task).filter(
        (Task.status != "в работе") & (Task.parent_task_id != None) & (Task.executor_id == None)
    ).all()

    result = []

    for task in important_tasks:
        least_busy_employee = db.query(Employee).outerjoin(Employee.tasks).group_by(Employee.id).order_by(
            func.count().asc()).first()

        if task.parent_task_id:
            parent_task_executor = db.query(Employee).join(Employee.tasks).filter(
                Task.id == task.parent_task_id).first()

            if parent_task_executor:
                if db.query(func.count()).filter(Task.executor_id == parent_task_executor.id).scalar() <= db.query(
                        func.count()).filter(Task.executor_id == least_busy_employee.id).scalar() + 2:
                    executor = parent_task_executor
                else:
                    executor = least_busy_employee
            else:
                executor = least_busy_employee
        else:
            executor = least_busy_employee

        task_info = {
            "employee_name": executor.name if executor else None,
            "task_name": task.name
        }

        result.append(task_info)

    return result


# Endpoint для "Занятых сотрудников"
@app.get("/busy_employees", response_model=List[EmployeeModel])
def busy_employees(db: Session = Depends(get_db)):
    busy_employees = db.query(Employee).join(Employee.tasks).group_by(Employee.id).order_by(
        func.count().desc()).all()

    return busy_employees


# Endpoint для назначения задачи сотруднику
@app.post("/assign_task", response_model=AssignedTask)
def assign_task(employee_id: int, task_id: int, db: Session = Depends(get_db)):
    employee = db.query(Employee).filter(Employee.id == employee_id).first()
    task = db.query(Task).filter(Task.id == task_id).first()

    if not employee or not task:
        raise HTTPException(status_code=404, detail="Сотрудник или задача не найдены")

    if task.status == "в работе":
        raise HTTPException(status_code=400, detail="Задача уже в работе")

    # Присвоение executor_id после назначения сотрудника
    task.executor_id = employee.id
    task.status = "в работе"
    db.commit()

    assigned_task = AssignedTask(employee_name=employee.name, task_name=task.name)
    return assigned_task
