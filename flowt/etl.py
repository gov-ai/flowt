import numpy as np

class DataCleaner:
    def __init__(self, remove_missing = True, replace_missing_dict=None):
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
        if (remove_missing is not None and remove_missing) and (replace_missing_dict is not None):
            raise ValueError('read the docstring note section')

        self.ret = True
        self._method = 'remove_missing' if (remove_missing == True) else 'replace_missing'
        self._replace_missing_dict = replace_missing_dict if self._method == 'replace_missing' else None

    def __call__(self, sample):
        """input to object of DataCleaned
        :@param sample: {str} input sample from scraper
        """
        splitted = sample.split(' ')
         
        try:
            close_price = float(splitted[0])
        except:
            close_price = self.deal_with_missing_value(self._method,'close_price',replace_dict= self._replace_missing_dict)

        try:
            change_per1 = float(splitted[1].replace('%', ''))
        except:
            change_per1 = self.deal_with_missing_value(self._method,'change_per1',replace_dict= self._replace_missing_dict)

        try:
            change_per2 =  float(splitted[3].replace('%', ''))
        except:
            change_per2 = self.deal_with_missing_value(self._method,'change_per2',replace_dict= self._replace_missing_dict)

        
        return (self.ret,None) if not self.ret else np.array([close_price, change_per1, change_per2]) 

    def deal_with_missing_value(self,method,sample_category,replace_dict = None):
        '''
        This function is called when there is an error in extracting values from raw string. 
        
        @param method: {str} 'remove_missing' or 'replace_missing' 
        @param sample_category: {str} 'close_price', 'change_per1' or 'change_per2' 
        @param replace_dict: {dict} if method == 'replace_missing'. 
        '''
        if method == 'remove_missing':
            self.ret = False
            return None
        else:
            return replace_dict[sample_category]



if __name__ == '__main__':

    # correct
    #cleaner1 = DataCleaner()
    #cleaner2 = DataCleaner(remove_missing=True)
    #cleaner3 = DataCleaner(remove_missing = False,replace_missing_dict=dict(close_price = 0,change_per1 = 0,change_per2 = 0))


    # wrong
    # cleaner4 = DataCleaner(remove_missing=True, replace_missing_dict=0)
    #cleaner1 = DataCleaner()

    # call
    #print(cleaner1("1.2090 0.0000 \xa0\xa0 0.00%"))
    #print(cleaner2("1.2090 0.0000 \xa0\xa0 0.00%"))
    #print(cleaner3("1.2090a 0.0000 \xa0\xa0 0.29%"))

    

