import threading


class threadingMan(threading.Thread):
    def __init__(self, func, args, name=''):
        threading.Thread.__init__(self)
        self.name = name
        self.func = func
        self.args = args
        self.res = ""

    def get_result(self):
        return self.res

    def run(self):
        self.res = self.func(*self.args)
