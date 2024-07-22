from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta

DATABASE_URL = "mysql+mysqlconnector://root:alam@localhost/jobsdb"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class Job(Base):
    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), index=True)
    description = Column(String(255))
    last_run = Column(DateTime, nullable=True)
    next_run = Column(DateTime, nullable=True)
    interval = Column(String(50))

Base.metadata.create_all(bind=engine)

app = FastAPI()

class JobCreate(BaseModel):
    name: str
    description: Optional[str] = None
    interval: str  

class JobRead(BaseModel):
    id: int
    name: str
    description: Optional[str]
    last_run: Optional[datetime]
    next_run: datetime
    interval: str

    class Config:
        orm_mode = True

@app.get("/jobs", response_model=List[JobRead])
def list_jobs(skip: int = 0, limit: int = 10):
    db = SessionLocal()
    jobs = db.query(Job).offset(skip).limit(limit).all()
    return jobs

@app.get("/jobs/{job_id}", response_model=JobRead)
def get_job(job_id: int):
    db = SessionLocal()
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

@app.post("/jobs", response_model=JobRead)
def create_job(job: JobCreate):
    db = SessionLocal()
    next_run = datetime.utcnow() + timedelta(days=1)  
    db_job = Job(name=job.name, description=job.description, next_run=next_run, interval=job.interval)
    db.add(db_job)
    db.commit()
    db.refresh(db_job)
    return db_job

@app.post("/jobs/run/{job_id}")
def run_job(job_id: int):
    db = SessionLocal()
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    job.last_run = datetime.utcnow()
    job.next_run = datetime.utcnow() + timedelta(days=1)  
    db.commit()
    return {"message": f"Job {job.name} executed successfully"}
