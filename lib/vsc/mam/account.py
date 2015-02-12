'''classes and utilities to represent and manipulate Adaptive MAM accounts'''

class MamAccount(object):
    '''class representing a MAM account'''

    def __init__(self, acc_id, name):
        '''constructor taking account ID, name'''
        self._id = acc_id
        self._name = name
        self._available = None
        self._allocated = None

    @property
    def account_id(self):
        '''returns MAM account ID'''
        return self._id

    @property
    def name(self):
        '''returns MAM account name'''
        return self._name

    @property
    def available_credits(self):
        '''returns number of credits available on this account'''
        return self._available

    @available_credits.setter
    def available_credits(self, credits):
        '''set the number of credits available on this account'''
        self._available = float(credits)

    @property
    def allocated_credits(self):
        '''returns total allocated credits for this account'''
        return self._allocated

    @allocated_credits.setter
    def allocated_credits(self, credits):
        '''set the total number of credits allocated on this account'''
        self._allocated = float(credits)

    def __str__(self):
        account_str = '{0} ({1}):'.format(self.name, self.account_id)
        account_str += '\n  available: {0:.2f}'.format(self.available_credits)
        account_str += '\n  allocated: {0:.2f}'.format(self.allocated_credits)
        return account_str
