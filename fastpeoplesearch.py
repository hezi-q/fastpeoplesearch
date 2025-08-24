import csv
from concurrent.futures import ThreadPoolExecutor
import json
import re
import requests
from threading import Lock

from bs4 import BeautifulSoup

from format import _format


lock = Lock()

parsed_urls = set()    


def save_data_jsonl(people, filename="fastpeoplesearch.jsonl"):
    with open(filename, "a", encoding="utf-8") as f:
        json_line = json.dumps(people, ensure_ascii=False)

        f.write(json_line + "\n")


def get(u, api_key, render=False, sel=''):
    url = f'https://api.scraperapi.com/?api_key={api_key}&url={u}&ultra_premium=true'
    if render:
        url = url + '&render=true'

    print(f'parsing: {url}')

    res = requests.get(url)
    soup =  BeautifulSoup(res.text, 'html.parser')
    
    if sel:
        if soup.select(sel):
            return soup
        else:
            return get(u, api_key, False, '')
    
    return soup


def parse_residents(address, url, api_key):
    soup = get(url, api_key, False, '.fullname')

    address['name'] = soup.select_one('.fullname').get_text(strip=True)

    landline = 0
    cellphone = 0

    for phone in soup.select('.detail-box-phone dt'):
        if not (phone.find_next_sibling('dd')): continue

        if 'Landline' in phone.find_next_sibling('dd').get_text():
            address[f"Landline {landline + 1}"] = phone.get_text(strip=True).lower().replace('(primary phone)', '')
            landline += 1

        elif 'Wireless' in phone.find_next_sibling('dd').get_text():
            address[f"Cellphone {cellphone + 1}"] = phone.get_text(strip=True).lower().replace('(primary phone)', '')
            cellphone+=1

    email_idx = 0
    for em in re.findall('[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', str(soup)):
        if 'fastpeoplesearch' not in em:
            address[f'Email {email_idx+1}'] = em
            email_idx += 1
    
    with lock:
        print(address)
        save_data_jsonl(address)


def parse_address(address, api_key):
    def address_helper(url, address, api_key):
        soup = get(url, api_key, False, '.card-title a')

        for resident in soup.select('.card-title a'):
            resident_url = resident.get('href')
            
            if resident_url not in parsed_urls:
                parsed_urls.add(resident_url)
                try:
                    parse_residents(address, f'https://www.fastpeoplesearch.com{resident_url}', api_key)
                except: pass

    url = f"https://www.fastpeoplesearch.com/address/{address['address'].replace(' ', '-')}_{address['city'].replace(' ', '-')}-{address['state']}-{address['zipcode']}"
    address_helper(url, address, api_key)
    

def main():
    with open('addresses.csv') as f2:
        reader = csv.DictReader(f2)

        scraper_api_key = open('scraper_api_key.txt').read().strip()
        
        with ThreadPoolExecutor(max_workers=100) as executor:
            for address in reader:
                executor.submit(parse_address, address, scraper_api_key)

    _format()


if __name__ == '__main__':
    main()
