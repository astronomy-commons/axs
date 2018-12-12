

class Constants:
    ONE_ASEC = 0.0002777777  # 1.0 / 3600.0
    ONE_AMIN = 0.0166666666  # 1.0 / 60.0
    NUM_BUCKETS = 500


from axs.catalog import AxsCatalog
from axs.axsframe import AxsFrame


__all__ = ['Constants', 'AxsCatalog', 'AxsFrame']

