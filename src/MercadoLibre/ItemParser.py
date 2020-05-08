import threading
import logging


from ..lib.Services import get_soup_from_url, clean_string


def split_status_amount(text):
    splitted_text = text.split(' - ')
    status = None
    amount = None
    if len(splitted_text) > 1:
        status, amount = splitted_text
        amount = amount.split(' ')[0]
    else:
        status = splitted_text[0]
    return (status, amount)


def parse_location(location_div):
    try:
        location = location_div.select(".card-description")[0]
        return location['title']
    except IndexError as err:
        logging.error("Error parsing location!", err)
        return "N/A"


def parse_specs_list(specs_list):
    try:
        return [{'spec_name': spec.strong.contents[0], 'spec_value': spec.span.contents[0]} for spec in specs_list]
    except IndexError as err:
        logging.error("Error getting item specs", specs_list)
        return []


def parse_description(description_ele):
    try:
        if len(description_ele) == 0:
            return ""
        contents = description_ele[0].p.contents
        clean_content = []
        for p in contents:
            # check if the variable is string, otherwise it could be a <br/> tag, just skip them
            is_str = isinstance(p, str)
            if is_str: clean_content.append(p)
        clean_content = " | ".join(clean_content)
        return clean_string(clean_content)
    except IndexError as err:
        logging.error("Error parsing item description", description_ele)
        return ""


class ItemParser(threading.Thread):
    def __init__(self, q, result):
        threading.Thread.__init__(self)
        self.result = result
        self.q = q

    def run(self):
        while not self.q.empty():
            item_work = self.q.get()  # fetch a new item from the queue
            full_item_info = {}
            try:
                logging.info("Requested work... %i" % item_work[0])
                item_soup = get_soup_from_url(item_work[1]['link'])
                # extract item info
                item_info = item_soup.select("#short-desc > div")[0]
                item_name = item_info.select(".item-title__primary")[0].contents[0]
                item_price = item_info.select(".price-tag-fraction")[0].contents[0]
                item_status_amount = item_info.select(".item-conditions")[0].contents[0]
                item_status_amount = clean_string(item_status_amount)
                extra_data = split_status_amount(item_status_amount)

                # extract seller location
                seller_location = item_soup.select(".seller-location")
                location = "N/A"
                if len(seller_location) > 0:
                    location = parse_location(seller_location[0])

                # extract item specs
                item_specs = item_soup.select(".specs-item")
                item_specs_list = parse_specs_list(item_specs)

                # extract item description
                item_description = item_soup.select(".item-description__text")
                item_description_text = parse_description(item_description)

                # extract id and metadata from item
                item_id = item_soup.select("#productInfo > input:nth-child(1)")[0]['value']
                parent_url = item_soup.select("#productInfo > input:nth-child(2)")[0]['value']

                full_item_info['item_name'] = clean_string(item_name)
                full_item_info['item_price'] = clean_string(item_price)
                full_item_info['sold_so_far'] = extra_data[1] if extra_data[0] else 0
                full_item_info['status'] = extra_data[0]
                full_item_info['location'] = location
                full_item_info['specs'] = item_specs_list
                full_item_info['characteristics'] = item_description_text
                full_item_info['id'] = item_id
                full_item_info['parent_url'] = parent_url
                full_item_info['original_url'] = item_work[1]['link']
                logging.info("Full item info %s: ", full_item_info)

                self.result[item_work[0]] = full_item_info
            except IndexError as err:
                logging.error('Error with URL: %s, error: %s', item_work[1]['link'], err)
                self.result[item_work[0]] = None
            self.q.task_done()
        return True
