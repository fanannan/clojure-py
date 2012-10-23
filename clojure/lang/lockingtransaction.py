

def runInTransaction(fn):
    return fn()


class LockingTransaction():
	pass
