import sched, time
from datetime import datetime, timedelta

def get_scheduler():
    return sched.scheduler(time.time, time.sleep)


if __name__ == '__main__':

    priority = 1
    time_interval = 1 # seconds
    time_limit = 5 # seconds
    tts = datetime.now() + timedelta(seconds=time_limit)


    def do_something(sc, tts): 
        # do your stuff
        print("Doing stuff...")

        # re-enter the cb cur-time is less than time-to-stop
        if datetime.now() < tts:
            sc.enter(time_interval, priority, do_something, (sc, tts))

    s = get_scheduler()
    s.enter(time_interval, priority, do_something, (s, tts))
    s.run()