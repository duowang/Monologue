import requests
import re
import csv
from datetime import datetime
from bs4 import BeautifulSoup

COMEDIAN_NAMES = {'Jay': "Jay Leno",
                  'Seth': 'Seth Meyers',
                  'Letterman': 'David Letterman',
                  'Kimmel': 'Jimmy Kimmel',
                  'Conan': 'Conan O\'Brian',
                  'Fallon': 'Jimmy Fallon',
                  'Corden': 'James Corden',
                  'Ferguson': "Craig Ferguson"}


def get_name(string):
    for name in COMEDIAN_NAMES:
        if len(re.findall(name, string)):
            return COMEDIAN_NAMES[name]


def monologue(page):
    monologue_dict = {}
    r = requests.get('http://www.newsmax.com/jokes/' + str(page))
    soup = BeautifulSoup(r.text, "html.parser")
    jokePage = soup.body.find('div', 'jokespage')
    dateStr = soup.body.find('div', 'jokesDate').text
    date = datetime.strptime(dateStr, "%A %b %d %Y").strftime("%Y-%m-%d")
    print page, date
    for comedian in jokePage.find_all('div', 'jokesHeader'):
        if 'jokesHeader' not in comedian.attrs['class']:
            break
        img_name = comedian.find('img').attrs.get('alt')
        comedian_name = get_name(img_name)
        monologue = comedian.find_next('p')
        # sometimes there is a <p> tag without closing part
        while monologue.name == 'p' and monologue.find('div', 'jokesHeader') is None:
            if len(monologue.text) > 10:
                txt = monologue.text.encode("utf-8")
                # print txt + " --"  + comedian_name
                monologue_dict.setdefault(comedian_name, []).append(txt)
            monologue = monologue.find_next()

    with open("newsmax/" + date + '.csv', 'w') as csvfile:
        fieldnames = ['name', 'monologue']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for name, jokes in monologue_dict.items():
            for joke in jokes:
                writer.writerow({"name": name, "monologue": joke})


if __name__ == '__main__':
    for i in range(1839, 3000):
        monologue(i)
