import requests
import re
from bs4 import BeautifulSoup


def get_soup_from_url(url):
    page = requests.get(url)
    return BeautifulSoup(page.content, 'html.parser')


def clean_string(string):
    return re.sub("\s+", ' ', string).strip()
