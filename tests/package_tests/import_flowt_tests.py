import os
import sys
from loguru import logger

print('[file]', __file__)


def import_installed_float_from_dist_test():
    try:
        exec('import flowt')
        logger.success(f'[Passed] Test 1')
    except:
        logger.exception(
            f'[Failed] Test 1')


if __name__ == '__main__':
    import_installed_float_from_dist_test()
