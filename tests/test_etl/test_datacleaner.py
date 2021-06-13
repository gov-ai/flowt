from flowt.etl import DataCleaner
import pytest
import numpy as np


@pytest.fixture
def cleaner():
    """ cleaner w/ default args """
    return DataCleaner()

@pytest.fixture
def rm_missing_cleaner():
    """ cleaner with remove missing=True """
    return DataCleaner(remove_missing=True)


class TestDataCleaner:

    def test_cleaner_initialisation(self, cleaner):
        assert cleaner is not None, "Test 1 Failed"

    def test_remove_missing_cleaner(self, rm_missing_cleaner):
        assert rm_missing_cleaner is not None, "Test 2.0 failed. Initilisation."
        
        raw_sample: str = "1.2090 0.0000 \u00\u01 0.0%"
        ret, cleaned = rm_missing_cleaner(raw_sample)

        assert type(ret) is bool, "Test 2.1 failed. `ret` type mismatch."
        assert type(cleaned) is np.ndarray, "Test 2.2 failed. `cleaned` type mismatch."        
        assert cleaned.shape is (1, 3),  "Test 2.3 failed. `cleaned` shape mismatch."
        assert cleaned.dtype is np.dtype('float64'), "Test 2.4 failed. `cleaned` dtype mismatch."