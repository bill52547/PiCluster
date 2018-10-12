class NotEqualLengthError(Exception):
    def __init__(self, names, lens):
        msg = "Lengths of lists or tuples are supposed to be equal, however get:\n"
        msg += "{:^32}{:^8}\n".format('Name or Array', 'Length')
        for n, l in zip(names, lens):
            msg += "{:^32}{:^8}\n".format(n, l)
        super(__class__, self).__init__(msg)

def assert_same_length(arrays, names=None):
    lens = [len(a) for a in arrays]
    if not all([l==lens[0] for l in lens]):
        if names is None:
            names = [repr(a) for a in arrays]
        raise NotEqualLengthError(names, lens)