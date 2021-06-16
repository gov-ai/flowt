from datetime import datetime, time
from os import name
class Scheduler:
    
    def __init__(self, time_interval='second'):
        accepted_intervals = ['minute', 'hour', 'second']
        if time_interval not in accepted_intervals:
            raise Exception(f"interval must be one of {['minute', 'hour', 'second']}")
        self.time_interval = time_interval
 
    def schedule(self, cb, args=[], kwargs={}):
        self._scheduled_cb = cb
        self._scheduled_cb_args = args
        self._scheduled_cb_kwargs = kwargs

    def run(self, limit=180, unit_max=60):
        visited = set()
        cntr_lim = 0

        if self.time_interval == 'second':
            time_hidx = 19
        elif self.time_interval == 'minute':
            time_hidx = 19-3
        elif self.time_interval == 'hour':
            time_hidx =19-3-3

        while True:
            cur = int(datetime.now().strftime("%Y-%m-%d %H:%M:%S")[time_hidx-2:time_hidx])
            if cntr_lim > (limit-1):
                break

            if  len(visited) == unit_max:
                visited = set()

            if cur in visited:
                continue

            # -----------------------------------------------------------------------
            # add business logic here
            # -----------------------------------------------------------------------
            self._scheduled_cb(*self._scheduled_cb_args, **self._scheduled_cb_kwargs)
            # -----------------------------------------------------------------------
            
            visited.add(cur)
            cntr_lim += 1

if __name__ == '__main__':

    acc = []
    def cb_test(a, b, c, acc):
        print('args:', a, b)
        print('kwrgs:', c)
        acc.append(1)
        print("---------------")

    s = Scheduler(time_interval='second')
    s.schedule(cb_test, [1,2], {'c': 3, 'acc': acc})

    s.run(limit=5)

    print(len(acc))