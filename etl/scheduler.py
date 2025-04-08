import schedule
import time
from etl.jobs.fetch_market_average_data.fetch_market_average_data import run

def schedule_jobs():
    """
    Schedule the fetch_live_market_average_data job to run daily at 4:00 PM ET.
    """
    schedule.every().day.at("16:00").do(run)  # U.S. stock market closing time

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    schedule_jobs()