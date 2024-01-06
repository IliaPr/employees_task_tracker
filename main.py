from fastapi import FastAPI, HTTPException, Depends
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


class EmployeeModel(BaseModel):
    name: str
    position: str


class TaskModel(BaseModel):
    name: str
    parent_task_id: Optional[int] = None
    executor_id: int
    deadline: datetime
    status: str


class AssignedTask(BaseModel):
    employee_name: str
    task_name: str


# Dependency to get the database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# CRUD operations for employees
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
    return {"message": "Employee deleted"}


# CRUD operations for tasks
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
    return {"message": "Task deleted"}


# Endpoint for "Важные задачи"
@app.get("/important_tasks", response_model=List[AssignedTask])
def important_tasks(db: Session = Depends(get_db)):
    important_tasks = db.query(Task).filter(
        (Task.status != "в работе") & (Task.parent_task_id != None)
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
            "employee_name": executor.name,
            "task_name": task.name
        }

        result.append(task_info)

    return result


# Endpoint for "Занятые сотрудники"
@app.get("/busy_employees", response_model=List[EmployeeModel])
def busy_employees(db: Session = Depends(get_db)):
    busy_employees = db.query(Employee).join(Employee.tasks).group_by(Employee.id).order_by(
        func.count().desc()).all()

    return busy_employees


# Endpoint for assigning a task to an employee
@app.post("/assign_task", response_model=AssignedTask)
def assign_task(employee_id: int, task_id: int, db: Session = Depends(get_db)):
    employee = db.query(Employee).filter(Employee.id == employee_id).first()
    task = db.query(Task).filter(Task.id == task_id).first()

    if not employee or not task:
        raise HTTPException(status_code=404, detail="Employee or Task not found")

    if task.status == "в работе":
        raise HTTPException(status_code=400, detail="Task is already in progress")

    task.executor = employee
    task.status = "в работе"
    db.commit()

    assigned_task = AssignedTask(employee_name=employee.name, task_name=task.name)
    return assigned_task


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
