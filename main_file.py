import schedule
import time

from sales import sales

class main_file:

    def __init__(self):
        pass

if __name__=='__main__':
    s=sales()
    s.create_tables()
    s.csv_to_temp()
    s.update_data()

    schedule.every().day.at("09:00").do(s.update_data)

    while True:
        schedule.run_pending()
        time.sleep(1)

