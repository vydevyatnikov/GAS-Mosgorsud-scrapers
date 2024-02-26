import os
import pickle
import pandas as pd
import requests
from bs4 import BeautifulSoup
import fake_headers
from urllib.request import urlretrieve
import aspose.words as aw
import time
import uuid6
import json
from urllib.error import HTTPError
from io import StringIO

pd.set_option("display.max_columns", None)


class MosgorsudScraper:

    def __init__(self, articles: list, output_dir: str, levels: list, temp_file_path: str = "C:/Users"):

        self.rename_dict = {"Уникальный идентификатор дела": "id",
                            "Номер дела ~ материала": "Номер дела (материала)",
                            "Подсудимый": "ФИО",
                            "Дата рассмотрения дела в первой инстанции": "Дата решения",
                            "Cудья": "judge"}

        self.articles = articles
        self.output_dir = output_dir
        self.temp_files_path = temp_file_path
        self.headers = fake_headers.Headers(headers=False).generate()
        self.raw_levels = levels
        self.levels = []
        self.add_levels()

        self.tasks = self.get_tasks()

        for article in self.articles:
            #for sub_task in self.tasks[article]:
            for i in range(len(self.tasks[article])):
                sub_task = self.tasks[article][i]
                if sub_task["status"] == "unfinished":
                    self.article_data = pd.read_csv(f"{self.output_dir}{article}{sub_task['level']}.csv",
                                                    index_col=0)
                    with open(f"{self.output_dir}{article}_{sub_task['level']}_cards.json",
                              "r", encoding="utf-8") as inp:
                        self.cards = json.loads(inp.read())
                    start = self.article_data.shape[0] / 15 + 1
                elif sub_task["status"] == "absent":
                    self.article_data = pd.DataFrame()
                    self.cards = {}
                    self.article_data.to_csv(f"{self.output_dir}{article}_{sub_task['level']}.csv")
                    with open(f"{self.output_dir}{article}_{sub_task['level']}_cards.json",
                              "w", encoding="utf-8") as outp:
                        json.dump(self.cards, outp)
                    self.tasks[article][i]["status"] = "unfinished"
                    start = 1
                else:
                    continue
                try:
                    n_of_pages = self.get_n_pages(article, sub_task["level"])
                    if n_of_pages is None:
                        continue
                    for page in range(int(start), n_of_pages + 1):
                        #if page == 10:
                        #    breakpoint()
                        print(f"Page #{page}")
                        gathered_links = False
                        page_cases_links = []
                        while not gathered_links:
                            page_cases_links = self.get_the_links(article, page, sub_task["level"])
                            if len(page_cases_links) == 0:
                                continue
                            else:
                                gathered_links = True
                        print("got_response")
                        self.get_the_data(page_cases_links)
                except Exception as exp:
                    breakpoint()
                    print(exp)
                    raise exp
                else:
                    self.tasks[article][i]["status"] = "finished"
                finally:
                    # self.article_data.rename(columns=self.rename_dict, inplace=True)
                    self.article_data.to_csv(f"{self.output_dir}{article}_{sub_task['level']}.csv")
                    with open(f"{self.output_dir}{article}_{sub_task['level']}_cards.json",
                              "w", encoding="utf-8") as outp:
                        json.dump(self.cards, outp)
                    with open(self.output_dir + 'tasks.pkl', "wb") as outp:
                        pickle.dump(self.tasks, outp, pickle.HIGHEST_PROTOCOL)

    def add_levels(self):
        if "Первая инстанция" in self.raw_levels:
            self.levels.append(1)
        if "Апелляция" in self.raw_levels:
            self.levels.append(2)
        if "Кассация" in self.raw_levels:
            self.levels.append(3)

    def get_tasks(self):
        if "tasks.pkl" in os.listdir(self.output_dir):
            with open(self.output_dir + 'tasks.pkl', "rb") as inp:
                return pickle.load(inp)
        else:
            tasks = {article: [] for article in self.articles}
            for article in self.articles:
                for level in self.levels:
                    tasks[article].append({"level": level, "status": "absent"})
            #return {i: "absent" for i in self.articles}
            return tasks

    def get_and_parse(self, link):
        while True:
            try:
                response = requests.get("https://mos-gorsud.ru" + link, headers=self.headers)
                if response.status_code != 200:
                    breakpoint()
            except Exception as excp:
                print(excp, link)
                continue
            else:
                break
        return BeautifulSoup(response.text, features="lxml"), response.text

    def get_the_links(self, article, page, level):
        page_links = []
        #if ', ' in article:
        #    article = article.replace(', ', '%2C+')
        response = requests.get(f"https://mos-gorsud.ru/search?codex={article}&instance={level}&processType=6&formType=fullForm&page={page}",
                                headers=self.headers)
        #table = pd.read_html(response.text)[0]
        parsed_html = BeautifulSoup(response.text, features="lxml")
        elems_w_links = parsed_html.find_all("nobr")[1:]
        links = []
        for i in elems_w_links:
            try:
                links.append(i.find("a").attrs["href"])
            except AttributeError:
                print("AttributeError", i)
            #time.sleep(2)
        page_links += links

        return page_links

    def get_the_data(self, links):
        print(len(links))
        temp_article_data = pd.DataFrame()
        temp_cards = {}
        for link in links:
            #print(link)
            parsed_response, response_text = self.get_and_parse(link)
            main_info_rows = parsed_response.find_all("div", attrs={"class": "row_card"})
            major_dict = {}
            #cards_dict = {}
            for row in main_info_rows:
                df_col = self.clear_string(row.find("div", attrs={"class": "left"}).text)
                df_row = self.clear_string(row.find("div", attrs={"class": "right"}).text)
                major_dict[df_col] = [df_row]
            major_dict["Наименование суда"] = [self.clear_string(parsed_response.find(
                                                "ul", attrs={"class": "breadcrumb"}).find_all("li")[1].text)]

            major_dict_data = pd.DataFrame(major_dict)
            major_dict_data.rename(columns=self.rename_dict, inplace=True)
            major_dict_data["unique_id"] = uuid6.uuid7().hex
            temp_cards[major_dict_data["unique_id"].iloc[0]] = self.get_additional_tables(response_text,
                                                                                          parsed_response)
            temp_article_data = pd.concat((temp_article_data, major_dict_data))
        #print(temp_article_data)
        self.article_data = pd.concat((self.article_data, temp_article_data))
        self.cards = {**self.cards, **temp_cards}
        print("gathered_data")

    def deal_with_files(self, parsed_response, table):
        table["texts"] = None
        # rows_w_links = parsed_response.find_all("div", attrs={"id": "act-documents"})[0].find_all("a")
        try:
            just_rows = parsed_response.find_all("div", attrs={"id": "act-documents"})[0].find_all("tr")
        except IndexError:
            return pd.DataFrame({"None": pd.Series(None)})
        for row_num in range(len(just_rows)):
            looking_for_links = just_rows[row_num].find_all('a')
            if len(looking_for_links) != 0:
                if len(looking_for_links) > 1:
                    breakpoint()
                # what_the_f_is_that = just_rows[row_num].find_all_previous()[1].text
                # print(what_the_f_is_that)
                # is_sentence = True if "приговор" in what_the_f_is_that[i].lower() else False  # и постановление?
                href = looking_for_links[0].attrs["href"]
                if href == "#":
                    continue
                text = self.get_text(href)
                table['texts'].iloc[(row_num-1)] = text
        return table

    def get_text(self, link_download):
        if link_download == "#":
            return None
        while True:
            try:
                urlretrieve("https://mos-gorsud.ru" + link_download, self.temp_files_path + ".doc")
                # Load DOC file
                doc = aw.Document(self.temp_files_path + ".doc")
            except HTTPError as e:
                if e.reason == "Internal Server Error":
                    return None
                else:
                    print(e, link_download)
                    time.sleep(10)
                    continue
            except Exception as excp:
                print(excp, link_download)
                time.sleep(10)
                continue
            else:
                # time.sleep(5)
                return doc.get_text()

    def get_additional_tables(self, response_text, parsed_response):
        temp_dict = {}
        tables = pd.read_html(StringIO(response_text))
        tables_names = [i.text for i in parsed_response.find_all("h3")]
        for tb in range(len(tables_names)):
            temp_dict[tables_names[tb]] = tables[tb].to_dict()
        table_w_text = self.deal_with_files(parsed_response, tables[(len(tables)-1)].copy())
        temp_dict["judicial_acts"] = table_w_text.to_dict()
        return temp_dict

    def get_n_pages(self, article, level):
        first_page_url = f'https://mos-gorsud.ru/search?codex={article}&instance={level}&processType=6&formType=fullForm&page=1'
        response = requests.get(first_page_url, headers=self.headers)
        soup = BeautifulSoup(response.text, features="lxml")
        try:
            return int(soup.find("input", attrs={"id": "paginationFormMaxPages"}).attrs["value"])
        except AttributeError:
            if soup.find('div', attrs={"class": "expapnd-table-btn"}) is not None:
                return 1
            else:
                return None

    @staticmethod
    def clear_string(string: str):
        return string.replace("  ", "").replace("\n", "")


if __name__ == "__main__":
    MosgorsudScraper(articles=["105"],
                     output_dir="path/to/output/directory/",
                     levels=["Первая инстанция", "Апелляция", "Кассация"],
                     temp_file_path="path/to/temporary/word/file")
