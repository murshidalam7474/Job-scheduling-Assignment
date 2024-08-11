from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import List, Optional
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
import schedule
import time
import threading
import pytz
#edit the below according to you db root name and password
DATABASE_URL = "mysql+mysqlconnector://{db_username}:{db_password}@localhost/updated_jobdb"

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
    day_of_week = Column(String(20), nullable=True)  # e.g., "Monday"
    time_of_day = Column(String(5), nullable=True)    # e.g., "21:35"

Base.metadata.create_all(bind=engine)

app = FastAPI()

class JobCreate(BaseModel):
    name: str
    description: Optional[str] = None
    day_of_week: Optional[str] = None  # e.g., "Monday"
    time_of_day: Optional[str] = None   # e.g., "21:35"

class JobRead(BaseModel):
    id: int
    name: str
    description: Optional[str]
    last_run: Optional[datetime]
    next_run: datetime
    day_of_week: Optional[str]
    time_of_day: Optional[str]

    class Config:
        orm_mode = True

def convert_to_india_time(utc_dt):
    kolkata_tz = pytz.timezone("Asia/Kolkata")
    return utc_dt.astimezone(kolkata_tz)

def job_function(job_id):
    db = SessionLocal()
    job = db.query(Job).filter(Job.id == job_id).first()
    if job:
        utc_now = datetime.now(pytz.utc)
        job.last_run = utc_now
        job.next_run = utc_now + timedelta(days=7)  # Schedule for next week
        db.commit()
        
        kolkata_last_run = convert_to_india_time(utc_now)
        print("Executed job {} at {} ".format(job.name, kolkata_last_run))
    db.close()

def schedule_job(job_id, day_of_week, time_of_day):
    def job_wrapper(job_id):
        job_function(job_id)

    schedule_day = {
        "Monday": schedule.every().monday,
        "Tuesday": schedule.every().tuesday,
        "Wednesday": schedule.every().wednesday,
        "Thursday": schedule.every().thursday,
        "Friday": schedule.every().friday,
        "Saturday": schedule.every().saturday,
        "Sunday": schedule.every().sunday
    }.get(day_of_week)

    if not schedule_day:
        raise ValueError("Invalid day of week: {}".format(day_of_week))

    hour, minute = map(int, time_of_day.split(':'))
    schedule_day.at("{:02d}:{:02d}".format(hour, minute)).do(job_wrapper, job_id)

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)

@app.on_event("startup")
def startup_event():
    db = SessionLocal()
    jobs = db.query(Job).all()
    for job in jobs:
        if job.day_of_week and job.time_of_day:
            schedule_job(job.id, job.day_of_week, job.time_of_day)
    db.close()
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()

@app.get("/jobs", response_model=List[JobRead])
def list_jobs(skip=0, limit=10):
    db = SessionLocal()
    jobs = db.query(Job).offset(skip).limit(limit).all()
    db.close()
    return jobs

@app.get("/jobs/{job_id}", response_model=JobRead)
def get_job(job_id):
    db = SessionLocal()
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    db.close()
    return job

@app.post("/jobs", response_model=JobRead)
def create_job(job: JobCreate, background_tasks: BackgroundTasks):
    db = SessionLocal()
    utc_now = datetime.now(pytz.utc)
    next_run = utc_now + timedelta(days=7)  # Schedule for next week
    db_job = Job(name=job.name, description=job.description, next_run=next_run,
                 day_of_week=job.day_of_week, time_of_day=job.time_of_day)
    db.add(db_job)
    db.commit()
    db.refresh(db_job)
    if job.day_of_week and job.time_of_day:
        background_tasks.add_task(schedule_job, db_job.id, job.day_of_week, job.time_of_day)
    db.close()
    
    kolkata_next_run = convert_to_india_time(next_run)
    print("Scheduled job {} to run at {} ".format(db_job.name, kolkata_next_run))
    return db_job

@app.post("/jobs/run/{job_id}")
def run_job(job_id):
    db = SessionLocal()
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    utc_now = datetime.now(pytz.utc)
    job.last_run = utc_now
    job.next_run = utc_now + timedelta(days=7)  # Schedule for next week
    db.commit()
    
    kolkata_last_run = convert_to_india_time(utc_now)
    print("Executed job {} at {} ".format(job.name, kolkata_last_run))
    
    db.close()
    return {"message": "Job {} executed successfully".format(job.name)}
