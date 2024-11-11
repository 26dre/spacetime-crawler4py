import math
from operator import __eq__
import sys
#from typing import Any, Callable, TypeAlias
from globals import url_string
import globals as gb
from urllib.parse import urlparse, urljoin, urldefrag, parse_qs
from test_suite import test_function

#parsed_url_dict: TypeAlias = dict

URL_SIMILARITY_THRESHOLD: float = .85


# def parse_url(url: url_string) -> parsed_url_dict:
#     parsed_url = urlparse(url)
#     return {
#         "scheme": parsed_url.scheme,
#         "netloc": parsed_url.netloc,
#         "path": parsed_url.path,
#         "params": parsed_url.params,
#         "query": parsed_url.query,
#         "fragment": parsed_url.fragment,
#         "hostname": parsed_url.hostname,
#         "port": parsed_url.port
#     }


def get_path_similarity(path1_list: list[str], path2_list: list[str]) -> int:

    print(f'path1_list = {path1_list}, len = {len(path1_list)}')
    consecutive_similar = 0

    for i in range(min(len(path1_list), len(path2_list))):
        print(i, end='\t')
        if path1_list[i] == path2_list[i]:
            consecutive_similar += 1
        else:
            break

    print()

    print(
        f'Found {consecutive_similar} consecutive similarities between list paths')
    return consecutive_similar


def path_similarity_up_to_last(path1_list: list[str], path2_list: list[str]) -> bool:
    if len(path1_list) == len(path2_list) and path1_list[:-1] == path2_list[:-1]:
        return True
    else:
        return False


def get_path_similarity_score(path1: str, path2: str) -> float:
    path1_list: list = [part for part in path1.split('/') if part]
    path2_list: list = [part for part in path2.split('/') if part]
    if not path_similarity_up_to_last(path1_list, path2_list):
        print(f'path1_list = {path1_list}')
        print(f'path2_list = {path2_list}')
        result: float = get_path_similarity(
            path1_list, path2_list) / max(len(path1_list), len(path2_list))
        return result
    else:
        first_string_set = set(path1_list[-1])
        second_string_set = set(path2_list[-1])
        union_length = len(first_string_set.union(second_string_set))
        intersection_length = len(
            first_string_set.intersection(second_string_set))

        return intersection_length / union_length


def get_path_length(path: str) -> int:
    len([part for part in path.split('/') if part])


def get_path_part_differences(path1: str, path2: str) -> int:
    num_difs: int = 0
    for char1, char2 in zip(path1, path2):
        if char1 != char2:
            num_difs += 1

    num_difs += abs(len(path1) - len(path2))

    return num_difs

# requires two paths of the same length


def get_last_part_of_paths_num_difs(path1: str, path2: str) -> int:
    path1_list: list = path1.split('/')
    path2_list: list = path2.split('/')
    if len(path1_list) != len(path2_list):
        return (
            max(
                len(path1_list),
                len(path2_list)

            )
        )

    return get_path_part_differences(path1=path1_list[-1], path2=path2_list[-1])


# def confirm_similarities_up_to_path(parsed_url1: parsed_url_dict, parsed_url2: parsed_url_dict) -> bool:
#     return (
#         parsed_url1["scheme"] == parsed_url2["scheme"] and
#         parsed_url1["netloc"] == parsed_url2["netloc"] and
#         parsed_url1["hostname"] == parsed_url2["hostname"] and
#         parsed_url1["port"] == parsed_url2["port"]
#     )


# if this is set to 2 there is no additional penalty for the same path + length
# if this is set to 1 there IS an additional penalty for the same path + length
NUM_DIFS_PENALTY: int = 1


# def get_link_similarity(url1: url_string, url2: url_string) -> float:

#     parsed_url1: parsed_url_dict = parse_url(url1)
#     parsed_url2: parsed_url_dict = parse_url(url2)

#     if not confirm_similarities_up_to_path(parsed_url1=parsed_url1, parsed_url2=parsed_url2):
#         return 0

    # if path_similarity_up_to_last()

    # the rest of this shit is queries and fucking fragments so i don't really care about checking those
    # if the website is the same (or too similar) then the rest of this is going to be the same too and should then also be removed
    # i should also probably add checking for if the difference count of the two paths is too similar
    # if get_path_length(parsed_url1["path"]) != get_path_length(parsed_url2["path"]):
    #     return (
    #         get_path_similarity(
    #             path1=parsed_url1["path"],
    #             path2=parsed_url2["path"]
    #         ) / min(
    #             get_path_length(parsed_url1["path"]),
    #             get_path_length(parsed_url2["path"])
    #         )
    #     )
    # else:
    #     return (
    #         (
    #             get_path_similarity(
    #                 path1=parsed_url1["path"],
    #                 path2=parsed_url2["path"]
    #             ) /
    #             get_path_length(parsed_url1["path"])
    #             -
    #             get_last_part_of_paths_num_difs(
    #                 path1=parsed_url1['path'],
    #                 path2=parsed_url2['path']) / get_path_length(parsed_url1["path"])
    #         )
    #         / NUM_DIFS_PENALTY
    #     )

#     return get_path_similarity_score(path1=parsed_url1['path'], path2=parsed_url2['path'])


# def should_evaluate_url(url1: url_string, url2: url_string, threshold: float = URL_SIMILARITY_THRESHOLD):
#     return get_link_similarity(url1=url1, url2=url2) < threshold


MAX_FLOAT_DIF_THRESHOLD: float = .01


# def test_url_differences_algorithm(url1: url_string, url2: url_string, expected_result: Any) -> bool:
#     return abs((get_link_similarity(url1, url2)) - expected_result) < MAX_FLOAT_DIF_THRESHOLD


# def test_function(expected_result: Any, result_evaluation_algorithm: function, function_to_test: function, *args: Any):

def local_isclose(float1: float, float2: float) -> bool:
    return math.isclose(a=float1, b=float2, rel_tol=MAX_FLOAT_DIF_THRESHOLD)


# def test_get_link_similarity():
#     print(f"Running in function: {sys._getframe().f_code.co_name}")
#     # result: bool = True

#     test_url1: url_string = "https://blogboard.io/blog/knowledge/python-print-to-stderr/"
#     test_url2: url_string = "https://blogboard.io/blog/knowledge/"
#     test_function(float(1), local_isclose,
#                   get_link_similarity, test_url1, test_url1)

#     test_function(.66, local_isclose, get_link_similarity,
#                   test_url1, test_url2)

#     # running tests with different urls below
#     test_url1: url_string = "https://blogboard.io/blog/knowledge/python-print-to-stderr/"
#     test_url2: url_string = "https://www.youtube.com/results?search_query=reform+part+1"
#     test_function(0, local_isclose, get_link_similarity,
#                   test_url1, test_url2)

#     test_url1: url_string = "https://sphinx.epic.com/hsch/"
#     test_url2: url_string = "https://epic.com/hsch"

#     test_function(0, local_isclose, get_link_similarity,
#                   test_url1, test_url2)

#     test_url1: url_string = "https://wics.ics.uci.edu/events/category/wics-bonding/day/2013-08-22/"
#     test_url2: url_string = "https://wics.ics.uci.edu/events/category/wics-bonding/day/2013-08-23/"

#     test_function(1, local_isclose, get_link_similarity, test_url1, test_url2)


# def test_should_evaluate_link_based_on_closeness():
#     print(f"Running in function: {sys._getframe().f_code.co_name}")

#     test_url1: url_string = "https://blogboard.io/blog/knowledge/python-print-to-stderr/"
#     test_url2: url_string = "https://blogboard.io/blog/knowledge/"
#     test_function(True, __eq__, should_evaluate_url, test_url1, test_url2)
#     test_url1: url_string = "https://stackoverflow.com/questions/16712795/pass-arguments-from-cmd-to-python-script"
#     test_url2: url_string = "https://stackoverflow.com/questions/16712795"

#     test_function(True, __eq__, should_evaluate_url, test_url1, test_url2)
#     test_url1: url_string = "https://wics.ics.uci.edu/events/category/wics-bonding/day/2013-08-22/"
#     test_url2: url_string = "https://wics.ics.uci.edu/events/category/wics-bonding/day/2013-08-23/"

#     test_function(False, __eq__, should_evaluate_url, test_url1, test_url2)


# def determine_if_should_evaluate_url_against_set(url_set: set[url_string], possible_new_url: url_string) -> bool:
#     for already_evaluated_url in url_set:
#         if not should_evaluate_url(url1=already_evaluated_url, url2=possible_new_url):
#             return False

#     return True


def go_thru_url_evaluation_phase_thread_safe(possible_new_url: url_string):
    return gb.read_global_variable(gb.unique_urls, gb.unique_urls_lock, determine_if_should_evaluate_url_against_set, possible_new_url)


RUN_SHOULD_EVAL_LINK_TEST: bool = True
RUN_TEST_GET_LINK_SIMILARITY: bool = True
# if __name__ == "__main__":

#     if RUN_TEST_GET_LINK_SIMILARITY:
#         test_get_link_similarity()
#     if RUN_SHOULD_EVAL_LINK_TEST:
#         test_should_evaluate_link_based_on_closeness()

#     # should return 1 for the next link, passing in the same set of two links
