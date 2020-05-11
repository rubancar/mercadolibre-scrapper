from dotenv import load_dotenv
# load environment variables before these imports
load_dotenv()

import requests
import logging
from bs4 import BeautifulSoup
from math import ceil
from queue import Queue
from src.lib.Services import clean_string
from src.MercadoLibre.ItemParser import ItemParser
from src.lib.db.sqlite import query_write, query_read, get_all_urls_one_by_one
from src.lib.db.elasticsearch import insert_data, prepare_payload


# This function extract the categories from Mercado Libre main page
def mercado_libre_extract_categories(doc):
    categories_url_dict = {}
    categories_section = doc.find('section', class_='categories')
    categories_list = categories_section.find_all('a', class_='category')
    # children from category list comes in tuple, extract each category link and name
    for category in categories_list:
        try:
            name = category.p.contents[0]
            category_url = category['href']
            categories_url_dict[name] = category_url
        except (IndexError, KeyError) as err:
            logging.error('Error while parsing category %s, category: %s', err, category)
    return categories_url_dict


def mercado_libre_extract_subcategories(doc, name):
    subcategories_dict = {}
    logging.debug('Starting categories extraction from sub-section %s ...', name)
    sub_categories = doc.select("#root-app > .categories > section > div > div:nth-child(2) > div > div "
                                "> div.group > div.categories__wrapper")
    for subcategory in sub_categories:
        try:
            # sub category title
            sub_cat_title = subcategory.h2.a
            sub_title = sub_cat_title.contents[0]
            sub_url = sub_cat_title['href']
            subcategories_dict[sub_title] = sub_url
        except (IndexError, KeyError) as err:
            print('Error while parsing subcategory (Error type):', err)
            print(subcategory)
    return subcategories_dict


elements_processed_so_far = 0
elements_pushed_to_elasticsearch_so_far = 0

def process_page_items(doc, subsection_name, section_name):
    global elements_processed_so_far, elements_pushed_to_elasticsearch_so_far
    q = Queue(maxsize=0)  # set up the queue to hold all the urls
    # if the page is ordered as a list: https://listado.mercadolibre.com.ec/acc-motos-cuatrimotos/
    items = doc.select("div.item__info > h2 > a")
    if len(items) == 0:
        logging.debug("Applying search type V2")
        # if the page is ordered as a list of images:
        # https://computacion.mercadolibre.com.ec/discos-duros-y-removibles/
        items = doc.select("a.item__info-link")
    # Use many threads (50 max, or one for each url)
    num_threads = min(50, len(items))
    # Populating Queue with tasks
    results = [{} for x in items]
    for i in range(len(items)):
        item = items[i]
        item_link = item['href']
        item_name = item.span.contents[0]
        q.put((i, {'name': item_name, 'link': item_link}))  # add the index and item information

    logging.debug("Starting workers")
    for i in range(num_threads):
        worker = ItemParser(q, results)
        worker.start()

    q.join()
    elements_processed_so_far += len(results)
    payload_to_elasticsearch = prepare_payload(results, subsection_name, section_name)
    elements_pushed_to_elasticsearch_so_far += len(payload_to_elasticsearch)
    insert_data(payload_to_elasticsearch)
    logging.info("Elements processed so far: %i, Elements pushed to ElasticSearch: %i",
                 elements_processed_so_far, elements_pushed_to_elasticsearch_so_far)


def get_elements_in_page(doc, subsection_url, subsection_name, section_name):
    try:
        # get quantity of elements
        quantity_of_items = doc.select(".quantity-results")[0].contents[0]
        # Mercado libre uses dot(.) as hundreds separator, so remove it
        quantity_of_items = clean_string(quantity_of_items).split(" ")[0].replace(".", "")
        logging.debug("Total number of items to lookup through: %s" % quantity_of_items)
        # 50 are the total number of items per page
        number_of_pages = int(ceil(int(quantity_of_items) / 50))
        logging.debug("Total number of pages to lookup through: %s" % number_of_pages)
        process_page_items(doc, subsection_name, section_name)
    except Exception as err:
        logging.error("Error processing 1st page of elements: %s, Error message: %s", subsection_url, err)
        return
    for i in range(1, number_of_pages, 1):
        try:
            item_from = (50*i) + 1
            if i == number_of_pages-1: logging.debug("Processing items %i to %s", item_from, quantity_of_items)
            else: logging.debug("Processing items %i to %i", item_from, item_from+49)
            # Mercado Libre pagination format: https://listado.mercadolibre.com.ec/acc-motos-cuatrimotos/_Desde_51
            item_from_text = "_Desde_" + str(item_from)
            next_url = subsection_url + item_from_text
            logging.debug("Next URL to process: %s", next_url)
            next_subsection_page = requests.get(next_url)
            next_subsection_soup = BeautifulSoup(next_subsection_page.content, 'html.parser')
            # preprocessing next message
            process_page_items(next_subsection_soup, subsection_name, section_name)
        except Exception as err:
            logging.error("Error processing next page: %s", err)
            continue


def main():
    logging.basicConfig(level=logging.INFO)
    logging.info('Starting Mercado Libre processing')
    err, sites = query_read('select * from sites', '')
    if err is not None:
        logging.error('There was an error with DB initialization %s', sites[0])
        return

    # for now we have just 1 site -> Mercado Libre
    # site structure => (id integer, name text, url text, n_visits integer)
    site_id, _, site_url, _ = sites[0]

    logging.debug('Site to be fetched %s', site_url)
    # request site
    page = requests.get(site_url)
    # convert site in a BeautifulSoup
    soup = BeautifulSoup(page.content, 'html.parser')
    # application root
    app_root_html = soup.find(id='root-app')
    # dictionary containing the category name as key and URL as its value
    categories = mercado_libre_extract_categories(app_root_html)

    '''###########               GET NEW CATEGORIES                    ##########'''
    for category_name, category_url in categories.items():
        try:
            err, category_in_db = query_read('select * from site_sections where name = ?', [category_name])
            # if category doesn't exists in DB, save the new one
            if len(category_in_db) == 0:
                logging.debug('New category found, Name:%s, URL:%s', category_name, category_url)
                # site_id integer, name text, url text, n_visits integer, total_elements integer, so_far_visit integer
                query_write('insert into site_sections (site_id, name, url, n_visits, total_elements, so_far_visit) '
                            'values (?,?,?,?,?,?)', [site_id, category_name, category_url, 0, 0, 0])
            else:
                logging.debug('Category already in DB, INFO %s', category_in_db)

        except TypeError as err:
            logging.error("Error in the following subcategory %s: %s", category_name, category_url)

    '''###########               GET NEW SUBCATEGORIES                    ##########'''
    err, categories_in_db = query_read('select * from site_sections', [])
    for category in categories_in_db:
        c_id, _, c_name, c_url, _, _, _ = category
        sub_category_page = requests.get(c_url)
        soup_subcategory = BeautifulSoup(sub_category_page.content, 'html.parser')
        # subcategories is a dict
        subcategories = mercado_libre_extract_subcategories(soup_subcategory, c_name)
        for subcat_name, subcat_url in subcategories.items():
            sub_category_full_name = c_name + "-" + subcat_name
            try:
                err, subcategory_in_db = query_read('select * from site_subsections where name = ?',
                                                    [sub_category_full_name])
                # if category doesn't exists in DB, save the new one
                if len(subcategory_in_db) == 0:
                    logging.debug('New subcategory found, Name:%s, URL:%s', subcat_name, subcat_url)
                    # site_section_id integer, name text, url text, n_visits integer, total_elements integer, so_far_visit integer
                    query_write(
                        'insert into site_subsections (site_section_id, name, url, n_visits, total_elements, so_far_visit) '
                        'values (?,?,?,?,?,?)', [c_id, sub_category_full_name, subcat_url, 0, 0, 0])
                else:
                    logging.debug('Subcategory already in DB %s', subcat_name)
            except TypeError as err:
                logging.error("Error in the following subcategory %s: %s", subcat_name, subcat_url)

    # get_all_rows_one_by_one returns a generator function
    urls_to_start_fetching = get_all_urls_one_by_one()
    # urls_to_start_fetching = ["https://computacion.mercadolibre.com.ec/discos-duros-y-removibles/"]
    for record in urls_to_start_fetching:
        logging.debug("URL to fetch data %s", record)
        # id, site_section_id, name, url, section_name
        subsection_id, section_id, subsection_name, subsection_url, section_name = record
        subsection_page = requests.get(subsection_url)
        subsection_soup = BeautifulSoup(subsection_page.content, 'html.parser')
        get_elements_in_page(subsection_soup, subsection_url, subsection_name, section_name)


if __name__ == '__main__':
    main()
