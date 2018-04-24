class Hodler:
    def __init__(self, address, balance):
        self.address = address
        self.balance = balance

    def __lt__(self, other):
        return self.balance < other.balance

    def __gt__(self, other):
        return self.balance > other.balance

    def __eq__(self, other):
        return self.balance == other.balance

    def as_list(self):
        return [self.address, self.balance]
