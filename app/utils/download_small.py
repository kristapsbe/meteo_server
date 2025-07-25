import pytz
import logging
import datetime

from download_utils import update_aurora_forecast


logging.basicConfig(
    filename='/data/download.log',
    filemode='a',
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s'
)


def do_20_m_download():
    logging.info("Aurora download job starting")
    update_time = datetime.datetime.now(pytz.timezone('Europe/Riga')).strftime("%Y%m%d%H%M")
    update_aurora_forecast(update_time)


if __name__ == "__main__":
    do_20_m_download()
