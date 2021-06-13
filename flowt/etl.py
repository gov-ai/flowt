import numpy as np

class DataCleaner:
    def __init__(self, remove_missing = None, replace_missing_dict=dict(close_price=np.nan, change_per1=np.nan, change_per2=np.nan)):
        """Take raw scraped string as input and return cleaned np.ndarray
        :@param remove_missing: {bool} removes missing samples.
            If raw input is missing/corrupted one of the values i.e it looks like
            "1.233 abc <some-uicode> 0.00", the whole sample must be discarded

            >> cleaner = DataCleaner(remove_missing=True)
            >> ret, cleaned = cleaner("1.233 abc <some-uicode> 0.0.0")
            >> ret # must be false because sample was corrupted
            False 
            >> cleaned # Nothing was returned as sample was dicarder
            None

        :@param replace_missing_dict: {dict} replaces missing with custom data
            replace the missing valued with help of dictionary
            
            >> cleaner = DataCleaner(replace_missing_dict=dict(close_price=0, change_per_1=np.nan, change_per_2=np.nan))
            >> ret, cleaned = cleaner("1.233 abc <some-unicode> 0.0.0")
            >> ret # must be true because corrupted data is always replaced
            True 
            >> cleaned
            np.array([[1.233, np.nan, np.nan]])

        Note: if `remove_missing` is given, `replace_missing_dict` should NOT be given as input to
        constructor. Similarly if `replace_missing_dict` is given, `remove_missing` should NOT be given 
        as input to constructor
        """
        if (remove_missing is not None) and (replace_missing_dict is not None):
            raise ValueError('read the docstring note section')
        
        if (remove_missing is None) and (replace_missing_dict is None):
            raise ValueError('pass atleast one argument')

        self._helper = remove_missing if remove_missing is not None else replace_missing_dict


    def __call__(self, sample):
        """input to object of DataCleaned
        :@param sample: {str} input sample from scraper
        """
        splitted = sample.split(' ')
        try:
            close_price = float(splitted[0])
        except:
            close_price = self._helper['close_price'] if (type(self._helper) is dict) else None

        try:
            change_per1 = float(splitted[1].replace('%', ''))
        except:
            change_per1 = self._helper['change_per1'] if type(self._helper) is dict else None

        try:
            change_per2 =  float(splitted[3].replace('%', ''))
        except:
            change_per2 = self._helper['change_per2'] if type(self._helper) is dict else None


        # wrong logic 
        if (type((self._helper)) is bool) and (self._helper is True):
            return [[close_price, change_per1, change_per2]]

        return np.ndarray([[close_price, change_per1, change_per2]]) 
    

if __name__ == '__main__':

    # correct
    cleaner2 = DataCleaner(remove_missing=True)
    cleaner3 = DataCleaner(replace_missing_dict=0)

    # wrong
    # cleaner4 = DataCleaner(remove_missing=True, replace_missing_dict=0)
    #cleaner1 = DataCleaner()

    # call
    cleaner1("'1.2090 0.0000 \xa0\xa0 0.00%'")

