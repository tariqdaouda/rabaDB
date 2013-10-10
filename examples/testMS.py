import collections

class A(collections.MutableSequence):

    def __init__(self, *args):
        self.list = list(args)

    def __len__(self): return len(self.list)

    def __getitem__(self, i): 
		try :
			return A(*self.list[i])
		except :
			return self.list[i]
			
    def __delitem__(self, i): del self.list[i]

    def __setitem__(self, i, v):
        self.list[i] = v

    def insert(self, i, v):
        self.list.insert(i, v)

    def __str__(self):
        return str(self.list)
		
s = A(1,2,3)
# lots of free methods
s[0] = 3
print s, type(s), type(s[1:2]), s[1:2], s[0]#len(s), bool(s), s.count(3), s.index(2), iter(s)
