import os
import pandas as pd
import time
import pickle
import json
import fake_headers
from bs4 import BeautifulSoup
import numpy as np
from requests.exceptions import JSONDecodeError
from math import ceil
import requests


def series_in(series, values_set):
    return pd.Series([True if i in values_set else False for i in series.values], index=series.index)


def create_folder(directory, article):
    try:
        os.mkdir(directory + f"{article}")
    except FileExistsError:
        for file in os.listdir(directory + f"{article}"):
            os.remove(directory + f"{article}" + "/" + file)


def jobs_generator(articles, dates, court_id: list, output_dir: str,
                   stages: tuple = ("Первая инстанция", "Апелляция", "Кассация")):
    '''
    Функция создает словарь для каждой статьи и помещает его в общий словарь.
    '''

    jobs = {article: [] for article in articles}
    for article in articles:
        create_folder(output_dir, article)

        if 0 in court_id:
            jobs[article].append({"dates": dates, "done": False, "court_id": 0,
                                  "n_of_pages": None, "stage": "Первая инстанция", 'n_of_cases_processed': 0})
        if 1 in court_id:
            for i in stages:
                jobs[article].append({"dates": dates, "done": False, "court_id": 1,
                                      "n_of_pages": None, "stage": i, 'n_of_cases_processed': 0})
    return jobs


class GASScraper:

    def __init__(self, output_dir, jobs=None):

        self.output_dir = output_dir
        self.court_specific_data = [("Уголовные дела мировых судей", "9627aab7-0ec7-4677-a3df-500782fa8739"),
                                    ("Уголовные дела", "7f9e8ff8-4bc8-46aa-bcbd-f2b3ed5f159f")]

        if jobs is None:
            with open(output_dir + "jobs.pkl", "rb") as inp:
                self.jobs = pickle.load(inp)
        else:
            self.jobs = jobs
        self.data_holder = pd.DataFrame()
        self.meta_holder = {}

        self.rng = np.random.default_rng()
        self.headers = fake_headers.Headers(headers=False).generate()
        self.waste = 0
        self.current_sub_job = {}
        self.files_num = None
        self.start_time = None
        self.timer = 6
        self.meta_holder_start_len = None
        self.data_holder_start_len = None
        self.sub_job_num = None
        self.ids_start_value = None
        self.ids_generator = None
        self.rename_dict = {"snippets": "Аннотация", "case_user_doc_number": "Номер дела (материала)",
                            "case_user_document_type": "Тип документа", "u_case_user_article": "Статья УК РФ",
                            "case_user_entry_date": "Дата поступления", "case_user_doc_result_date": "Дата решения",
                            "case_doc_subject_rf": "Субъект РФ", "case_user_doc_court": "Наименование суда",
                            "u_common_case_defendant_name": "ФИО", "case_user_doc_result": "Результат"}

    def master(self):
        try:
            for job in self.jobs:
                for sub_job_num in range(len(self.jobs[job])):
                    sub_job = self.jobs[job][sub_job_num]
                    self.current_sub_job = sub_job
                    self.sub_job_num = sub_job_num
                    if not self.jobs[job][sub_job_num]["done"]:
                        self.files_num = self.get_files_num(job)
                        self.load_files(job)
                        self.data_holder_start_len = self.data_holder.shape[0]
                        self.meta_holder_start_len = len(
                            self.meta_holder[self.jobs[job][self.sub_job_num]["court_id"]])
                        try:
                            self.ids_master(job, sub_job["court_id"], sub_job["stage"])
                        except (StopIteration, RuntimeError):
                            sub_job["done"] = True
                        except Exception as excp:
                            breakpoint()
                            raise excp
                        else:
                            try:
                                self.text_master(job)
                            except Exception as excp:
                                breakpoint()
                                raise excp
                            else:
                                sub_job["done"] = True
                        finally:
                            self.dump_jobs(job)
                            self.dump_the_data(job)

        except Exception as excp:
            breakpoint()
            raise excp
        else:
            print("Done")
        finally:
            print(self.waste)

    def get_files_num(self, job, limit=3, create_new=True):
        stage = self.current_sub_job["stage"]
        for i in range(0, 10 ** 5):
            if f"{job}_data_{stage}_{i}.csv" in os.listdir(self.output_dir + f"{job}/"):
                if (os.path.getsize(self.output_dir + f"{job}/{job}_data_{stage}_{i}.csv") / 1024 ** 3 < limit and
                        os.path.getsize(self.output_dir + f"{job}/{job}_cards_{stage}_{i}.pkl") / 1024 ** 3 < limit):
                    return i, stage
            elif create_new:
                pd.DataFrame().to_csv(self.output_dir + f"{job}/{job}_data_{stage}_{i}.csv")
                with open(self.output_dir + f"{job}/{job}_cards_{stage}_{i}.pkl", "wb") as outp:
                    pickle.dump({0: {}, 1: {}}, outp, pickle.HIGHEST_PROTOCOL)
                    return i, stage

    def load_files(self, job, files_num=None):
        if files_num is None:
            files_num = self.files_num
        with open(self.output_dir + f"{job}/{job}_data_{files_num[1]}_{files_num[0]}.csv", "rb") as inp:
            self.data_holder = pd.read_csv(inp, index_col=0)
        with open(self.output_dir + f"{job}/{job}_cards_{files_num[1]}_{files_num[0]}.pkl", "rb") as inp:
            self.meta_holder = pickle.load(inp)

    def dump_the_data(self, job, files_num=None):
        if files_num is None:
            files_num = self.files_num
        self.data_holder.to_csv(self.output_dir + f"{job}/{job}_data_{files_num[1]}_{files_num[0]}.csv")
        with open(self.output_dir + f"{job}/{job}_cards_{files_num[1]}_{files_num[0]}.pkl", "wb") as outp:
            pickle.dump(self.meta_holder, outp, pickle.HIGHEST_PROTOCOL)

    def dump_jobs(self, job):
        self.jobs[job][self.sub_job_num]["n_of_cases_processed"] += (self.data_holder.shape[0] -
                                                                     self.data_holder_start_len)
        with open(self.output_dir + "jobs.pkl", "wb") as outp:
            pickle.dump(self.jobs, outp, pickle.HIGHEST_PROTOCOL)

    def intermediate_dump(self, job):
        self.dump_the_data(job)
        self.dump_jobs(job)
        check_files_num = self.get_files_num(job)
        if check_files_num[0] != self.files_num[0]:
            # is it faster?
            del self.meta_holder
            del self.data_holder
            self.files_num = check_files_num
            self.meta_holder = {0: {}, 1: {}}
            self.data_holder = pd.DataFrame()
            self.data_holder_start_len = 0
            self.meta_holder_start_len = 0
        else:
            self.data_holder_start_len = self.data_holder.shape[0]
            self.meta_holder_start_len = len(self.meta_holder[self.jobs[job][self.sub_job_num]["court_id"]])

    def ids_master(self, job, court_id, stage):
        self.ids_start_value = self.ids_get_start(job, court_id, stage)
        self.ids_generator = self.ids_gen()
        self.start_time = time.time()
        for page in self.ids_generator:
            if (time.time() - self.start_time) / 3600 >= self.timer:
                self.intermediate_dump(job)
            self.gather_ids(page, job, court_id, stage)
            time.sleep(6)

    def text_master(self, job):
        for ids in self.data_holder.loc[pd.isnull(self.data_holder["texts"]), "id"]:
            self.gather_texts_and_cards(ids, job)
            time.sleep(6)

    def ids_gen(self):
        for i in range(self.ids_start_value, self.current_sub_job["n_of_pages"]):
            yield i

    def ids_get_start(self, article, court_id, stage):
        if self.current_sub_job["n_of_pages"] is None:
            if court_id == 0:
                stage_part = ""
            else:
                stage_part = (',{\"name\":\"case_doc_instance\",\"operator\":\"EX\",\"query\":\"' +
                              stage + '\",\"sQuery\":null}')
            page_is_not_accessible = True
            while page_is_not_accessible:
                try:
                    smth = json.dumps({"request": {"groups": [self.court_specific_data[court_id][0]],
                                                   "sorts": [{"field": "score", "order": "desc"}], "type": "MULTIQUERY",
                                                   "multiqueryRequest": {"queryRequests": [{"type": "Q",
                                                                                            "request": '{\"mode\":\"EXTENDED\",\"typeRequests\":[{\"fieldRequests\":[{\"name\":\"u_case_user_article\",\"operator\":\"EX\",\"query\":\"Статья  ' + str(
                                                                                                article) + '\",\"sQuery\":null}],\"mode\":\"AND\",\"name\":\"Уголовные дела\",\"typesMode\":\"AND\"},{\"fieldRequests\":[{\"name\":\"case_user_doc_result_date\",\"operator\":\"B\",\"query\":\"' + str(
                                                                                                self.current_sub_job[
                                                                                                    "dates"][
                                                                                                    0]) + '-01-01T00:00:00\",\"sQuery\":\"' + str(
                                                                                                self.current_sub_job[
                                                                                                    "dates"][
                                                                                                    1]) + '-12-31T00:00:00\",\"fieldName\":\"case_user_doc_result_date\"}' + stage_part + '],\"mode\":\"AND\",\"name\":\"common\",\"typesMode\":\"AND\"}]}',
                                                                                            "operator": "AND",
                                                                                            "queryRequestRole": "CATEGORIES"},
                                                                                           {"type": "SQ", "queryId":
                                                                                               self.court_specific_data[
                                                                                                   court_id][1],
                                                                                            "operator": "AND"}]},
                                                   "simpleSearchFieldsBundle": None, "start": 0, "rows": 10,
                                                   "uid": "562dc91c-a053-4715-b851-c18e8197553f", "noOrpho": False,
                                                   "facet": {"field": ["type"]}, "facetLimit": 21,
                                                   "additionalFields": ["case_user_doc_number",
                                                                        "case_user_document_type",
                                                                        "u_case_user_article", "case_user_entry_date",
                                                                        "case_user_doc_result_date",
                                                                        "case_doc_subject_rf", "case_user_doc_court",
                                                                        "u_common_case_defendant_name",
                                                                        "case_user_doc_result"], "hlFragSize": 1000,
                                                   "groupLimit": 3, "woBoost": False}, "doNotSaveHistory": False})
                    smth = json.loads(smth)
                    response_raw = requests.post(url="https://bsr.sudrf.ru/bigs/s.action", json=smth,
                                                 headers=self.headers)

                    response = pd.json_normalize(response_raw.json())
                except Exception as exp:
                    print(exp)
                    continue
                else:
                    page_is_not_accessible = False
            n_of_cases = int(response["searchResult.shards"][0][0]["numFound"])
            self.current_sub_job["n_of_pages"] = ceil(n_of_cases / 20)
            print("Done w ids_get_start")
            start = 0
        else:
            print("Done w ids_get_start")
            start = self.current_sub_job["n_of_cases_processed"] // 20
        return start

    def gather_ids(self, i, article, court_id, stage):
        print("started_ids_gathering")
        if court_id == 0:
            stage_part = ""
        else:
            stage_part = ',{\"name\":\"case_doc_instance\",\"operator\":\"EX\",\"query\":\"' + stage + '\",\"sQuery\":null}'
        smth = json.dumps({"request": {"groups": [self.court_specific_data[court_id][0]],
                                       "sorts": [{"field": "score", "order": "desc"}], "type": "MULTIQUERY",
                                       "multiqueryRequest": {"queryRequests": [{"type": "Q",
                                                                                "request": '{\"mode\":\"EXTENDED\",\"typeRequests\":[{\"fieldRequests\":[{\"name\":\"u_case_user_article\",\"operator\":\"EX\",\"query\":\"Статья  ' + str(
                                                                                    article) + '\",\"sQuery\":null}],\"mode\":\"AND\",\"name\":\"Уголовные дела\",\"typesMode\":\"AND\"},{\"fieldRequests\":[{\"name\":\"case_user_doc_result_date\",\"operator\":\"B\",\"query\":\"' + str(
                                                                                    self.current_sub_job["dates"][
                                                                                        0]) + '-01-01T00:00:00\",\"sQuery\":\"' + str(
                                                                                    self.current_sub_job["dates"][
                                                                                        1]) + '-12-31T00:00:00\",\"fieldName\":\"case_user_doc_result_date\"}' + stage_part + '],\"mode\":\"AND\",\"name\":\"common\",\"typesMode\":\"AND\"}]}',
                                                                                "operator": "AND",
                                                                                "queryRequestRole": "CATEGORIES"},
                                                                               {"type": "SQ",
                                                                                "queryId":
                                                                                    self.court_specific_data[court_id][
                                                                                        1],
                                                                                "operator": "AND"}]},
                                       "simpleSearchFieldsBundle": None, "start": 20 * i, "rows": 20,
                                       "uid": "562dc91c-a053-4715-b851-c18e8197553f", "noOrpho": False,
                                       "facet": {"field": ["type"]}, "facetLimit": 21,
                                       "additionalFields": ["case_user_doc_number", "case_user_document_type",
                                                            "u_case_user_article", "case_user_entry_date",
                                                            "case_user_doc_result_date", "case_doc_subject_rf",
                                                            "case_user_doc_court",
                                                            "u_common_case_defendant_name",
                                                            "case_user_doc_result"], "hlFragSize": 1000,
                                       "groupLimit": 3, "woBoost": False}, "doNotSaveHistory": False})
        smth = json.loads(smth)
        while True:
            try:
                print("attempting_post_ids")
                response_raw = requests.post(url="https://bsr.sudrf.ru/bigs/s.action", json=smth, headers=self.headers)
                response = pd.json_normalize(response_raw.json())
                print("got_results_ids")
                docs = response['searchResult.documents'][0]

                if len(docs) != 20:
                    if i == self.current_sub_job["n_of_pages"] - 1:
                        pass
                    else:
                        breakpoint()
                        continue
                temp_data = pd.DataFrame()
                for case in docs:
                    temp_dict = {"id": [case["id"]]}
                    temp_snippet = str()
                    for j in range(len(case["snippets"])):
                        temp_snippet += case["snippets"][j].replace("<em>", "").replace("</em>", "") + "---"
                    temp_dict["snippets"] = temp_snippet
                    for field in case['additionalFields']:
                        if field["valueWOHL"] is not None:
                            if field["name"] == 'u_common_case_defendant_name' and field["name"] in temp_dict:
                                temp_dict[field["name"]][0] += "; " + field["valueWOHL"]
                            else:
                                temp_dict[field["name"]] = [field["valueWOHL"]]
                    temp_df = pd.DataFrame(temp_dict)
                    temp_df["criminal_court"] = court_id
                    temp_df["stage"] = stage
                    temp_df["texts"] = np.NaN
                    temp_df.rename(columns=self.rename_dict, inplace=True)
                    temp_data = pd.concat((temp_data, temp_df))
                temp_data.index = pd.Index(range(i * 20 + 1, i * 20 + 1 + temp_data.shape[0]))
                self.data_holder = pd.concat((self.data_holder, temp_data))
            except (JSONDecodeError, ConnectionResetError) as exp:
                print(exp)
                self.waste += 2
                time.sleep(10)
                continue
            except KeyError as excp:
                print(excp)
                # breakpoint()
            else:
                print("Gathered id")
                return

    def gather_texts_and_cards(self, case_id, job):
        print(case_id + " beginning")
        court_id = self.current_sub_job["court_id"]
        temp_article = {"type": "Q",
                                      "request": '{\"mode\":\"EXTENDED\",\"typeRequests\":[{\"fieldRequests\":[{\"name\":\"u_case_user_article\",\"operator\":\"EX\",\"query\":\"Статья ' + job + '\",\"sQuery\":null}],\"mode\":\"AND\",\"name\":\"Уголовные дела\",\"typesMode\":\"AND\"},{\"fieldRequests\":[{\"name\":\"case_user_doc_result_date\",\"operator\":\"B\",\"query\":\"2000-01-01T00:00:00\",\"sQuery\":\"2023-12-31T00:00:00\",\"fieldName\":\"case_user_doc_result_date\"}],\"mode\":\"AND\",\"name\":\"common\",\"typesMode\":\"AND\"}]}',
                                      "operator": "AND", "queryRequestRole": "CATEGORIES"}
        smth = json.dumps(
            {"request": {"sorts": [{"field": "score", "order": "desc"}], "type": "MULTIQUERY", "multiqueryRequest": {
                "queryRequests": [temp_article,
                                  {"type": "SQ", "queryId": self.court_specific_data[court_id][1],
                                   "operator": "AND"}]},
                         "simpleSearchFieldsBundle": None, "noOrpho": False,
                         "groups": [self.court_specific_data[court_id][0]],
                         "id": case_id, "shards": [self.court_specific_data[court_id][0]],
                         "hlColors": ["searchHL0"], "uid": "562dc91c-a053-4715-b851-c18e8197553f"}, "saveBoostQuery": False,
             "oneFieldName": None})
        smth = json.loads(smth)

        while True:
            try:
                response = requests.post("https://bsr.sudrf.ru/bigs/showDocument.action",
                                         json=smth, headers=self.headers)

                case_data = pd.json_normalize(response.json())
                try:
                    case_data = pd.json_normalize(case_data["document.fields"][0])
                except KeyError:
                    continue
                try:
                    case_text = case_data[case_data['comment'] == 'Текст документа']['value'].item()
                except ValueError:
                    pass
                else:
                    case_text = BeautifulSoup(case_text, features="lxml").text.strip()
                    self.data_holder.loc[(self.data_holder["id"] == case_id) &
                                         (self.data_holder["criminal_court"] == court_id), "texts"] = case_text
                    case_data.loc[case_data['comment'] == 'Текст документа', 'value'] = np.NaN
                    case_data.loc[case_data['comment'] == 'Текст документа', 'valueWOHL'] = np.NaN
                judge = case_data.loc[case_data["name"] == "case_user_judge", "value"]
                if len(judge) == 1:
                    self.data_holder.loc[(self.data_holder["id"] == case_id) &
                                         (self.data_holder["criminal_court"] == court_id),
                                         "judge"] = judge.iloc[0]
                elif len(judge) > 1:
                    judge_string = ""
                    judge = judge.str.strip()
                    for j in judge.index:
                        judge.loc[j] = " ".join(judge.loc[j].split("  "))
                        if len(judge.loc[j]) > 1 and judge.loc[j] not in judge_string:
                            if len(judge_string) == 0:
                                judge_string += judge.loc[j]
                            else:
                                judge_string += "---" + judge.loc[j]

                case_data["case_id"] = case_id
                case_data["criminal_court"] = court_id
                # temp_time = time.time()
                self.meta_holder[self.current_sub_job["court_id"]][case_id] = case_data

            except (JSONDecodeError, ConnectionResetError) as exp:
                print(exp)
                self.waste += 2
                time.sleep(10)
                continue
            else:
                print("Gathered text")
                return


if __name__ == "__main__":
    frank = GASScraper(output_dir="path/to/output/dir",
                       jobs=jobs_generator(articles=["228.2"],
                                           dates=["2000", "2024"], court_id=[0, 1],
                                           output_dir="path/to/output/dir/fot/jobs/"))
    frank.master()
