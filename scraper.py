import re
# from typing import Any, Callable, TypeAlias
from urllib.parse import urlparse, urljoin, urldefrag, parse_qs
from bs4 import BeautifulSoup
import random
from collections import deque
import globals
from globals import (Token, Token_Tuple, HASH, url_string)
from tokenizer import tokenize
import ngrams
import link_similarity
import save_data
import sys
INCLUDE_N_GRAMS_PHASE: bool = True
INCLUDE_URL_SIMILARITY_CHECKING: bool = False
saving_count = 0

# Define the function to filter out stop words


def filter_stop_words(tokens):
    # Define a list of English stop words
    stop_words = set([
        'a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an',
        'and', 'any', 'are', 'as', 'at', 'be', 'because', 'been', 'before',
        'being', 'below', 'between', 'both', 'but', 'by', 'could', 'did', 'do',
        'does', 'doing', 'down', 'during', 'each', 'few', 'for', 'from',
        'further', 'had', 'has', 'have', 'having', 'he', 'her', 'here', 'hers',
        'herself', 'him', 'himself', 'his', 'how', 'i', 'if', 'in', 'into',
        'is', 'it', "it's", 'its', 'itself', 'just', 'me', 'more', 'most',
        'my', 'myself', 'no', 'nor', 'not', 'of', 'off', 'on', 'once', 'only',
        'or', 'other', 'our', 'ours', 'ourselves', 'out', 'over', 'own', 's',
        'same', 'she', "she's", 'should', 'so', 'some', 'such', 't', 'than',
        'that', "that's", 'the', 'their', 'theirs', 'them', 'themselves',
        'then', 'there', 'these', 'they', 'this', 'those', 'through', 'to',
        'too', 'under', 'until', 'up', 'very', 'was', 'we', 'were', 'what',
        'when', 'where', 'which', 'while', 'who', 'whom', 'why', 'will', 'with',
        'you', 'your', 'yours', 'yourself', 'yourselves'
    ])

    filtered_tokens = [token for token in tokens if token not in stop_words]
    return filtered_tokens

# Define the is_valid function to filter URLs


def is_valid(url):
    try:
        # Remove fragment, if any
        url_defrag = urldefrag(url).url

        parsed = urlparse(url_defrag)

        # Check if the scheme is http or https
        if (not parsed.scheme in {"http", "https"} or parsed.hostname is None or parsed.netloc is None
            or "?" in url or "&" in url
        ):
            return False

        # Extract components
        netloc = parsed.netloc.lower()
        path = parsed.path.lower()
        query = parsed.query.lower()

        if not any(parsed.hostname.endswith(domain) for domain in globals.allowed_domains):
            return False
        
        if any(fragment in url for fragment in ["#comment", "#comments", "#respond", "redirect"]):
            return False

        # Check if the netloc is one of the allowed domains
        
            # Additional check for today.uci.edu
            # if "today.uci.edu" in netloc:
            #     if not path.startswith(globals.today_uci_edu_path):
            #         return False

            # Exclude disallowed domains
        disallowed_domains = {"gitlab.ics.uci.edu",
                                "swiki.ics.uci.edu", "wiki.ics.uci.edu"}
        if netloc in disallowed_domains or any(domain in parsed.hostname for domain in disallowed_domains):
            return False
        

        if re.match(r"^.*?(/.+?/).*?\1.*$|^.*?/(.+?/)\2.*$", path):
            return False 

        # Exclude URLs with disallowed file extensions
        if (re.search(r"/(search|login|news|logout|api|admin|raw|git|pix|static|calendar|event)/", path) or
            re.search(r"/(page|p)/?\d+", path) or
            re.search(r"(sessionid|sid|session)=[\w\d]{32}", query) or #maybe add login?
            re.search(r"p=iot", query) or
            re.search(r"pix", query) or
            re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            r"|png|tiff?|mid|mp2|mp3|mp4"
            r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            r"|epub|dll|cnf|tgz|sha1"
            r"|thmx|mso|arff|rtf|jar|csv"
                r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", path)):
            return False

        # Exclude URLs with dates
        if (re.search(r"(?:\d{4}[-\/]\d{1,2}[-\/]\d{1,2})", path) or
                    re.search(r"(?:\d{1,2}[-\/]\d{1,2}[-\/]\d{2,4})", path) or
                    re.search(
                    r"(?:\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s\d{1,2},\s\d{4})", path)
                ):
            return False
        
        if (re.search(r"(?:\d{4}[-\/]\d{1,2}[-\/]\d{1,2})", query) or
                    re.search(r"(?:\d{1,2}[-\/]\d{1,2}[-\/]\d{2,4})", query) or
                    re.search(
                    r"(?:\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s\d{1,2},\s\d{4})", query)
                ):
            return False

        # Exclude URLs with excessive query parameters
        if len(parse_qs(query)) > 2:
            return False

        # Exclude URLs with specific query parameters
        disallowed_params = {'do', 'tab_details',
                                'tab_files', 'image', 'ns'}
        query_params = set(parse_qs(query).keys())
        if disallowed_params.intersection(query_params):
            return False

        # Limit the number of query parameters
        if len(query_params) > 2:
            return False

        # Exclude URLs with repetitive patterns
        if has_repetitive_pattern(url):
            return False

        return True
        
    except TypeError:
        print("TypeError for ", url)
        return False


def has_repetitive_pattern(url):
    parsed = urlparse(url)
    path = parsed.path.strip('/')
    segments = path.split('/')

    # Check for repetition in path segments
    if len(segments) != len(set(segments)):
        return True

    # Check for repetitive query parameters
    query = parsed.query
    params = parse_qs(query)
    if len(params) != len(set(params)):
        return True

    return False

# Define the function to extract next links from the page

def word_conversion(response_content):
    try:
        if isinstance(response_content, bytes):
            response_content = response_content.decode('utf-8', errors='ignore')
        # Parse the HTML content
        soup = BeautifulSoup(response_content, 'html.parser')
        
        for style in soup(['script', 'style']):
            style.decompose()
        
        # Extract visible text from the page
        text = soup.get_text(separator=' ', strip=True)

        # Tokenize the text content
        tokens = tokenize(text)
        
        return tokens

    except Exception as e:
        print(f"Error processing response content: {e}")
        return []

def extract_next_links(url, resp):
    links = []

    # Check if the response is valid
    try:
        if not resp or not resp.raw_response.content:
            print("Early Exit: missing content response.")
            return links
        if resp.status and not (200 <= resp.status < 400):
            print("Early Exit: Invalid URL status")
            return links
        if len(resp.raw_response.content) < 400:
            print("Early Exit: Low information")
            return links
        if len(word_conversion(resp.raw_response.content)) <= 100:
            print("Early Exit: Low information")
            return links
    except Exception as e:
        print(f"Exception occurred {e}")
        return links

    # Check if the Content-Type is text/html
    content_type = resp.raw_response.headers.get('Content-Type', '').lower()
    if not content_type or not content_type.startswith('text/html'):
        return links

    try:
        # Parse the HTML content
        soup = BeautifulSoup(resp.raw_response.content, 'lxml')

        # Find all <a> tags with href attributes
        for tag in soup.find_all('a', href=True):
            href = tag.get('href')

            if href:
                # Ignore JavaScript links
                if href.lower().startswith('javascript:'):
                    continue

                # Resolve relative URLs to absolute URLs
                absolute_url = urljoin(url, href)

                # Remove fragment identifiers
                absolute_url, _ = urldefrag(absolute_url)

                # Check if the URL has a valid scheme
                parsed_href = urlparse(absolute_url)
                if parsed_href.scheme in {'http', 'https'}:
                    links.append(absolute_url)

    except Exception as e:
        print(f"Error parsing {url}: {e}")

    return links
# Define the main scraper function

filename = 'data.txt'

def saveFile():
    print_out = ""
    print_out += f"Total unique pages: {len(globals.unique_urls)}\n"

    # Print the longest page info
    print_out += f"Longest page URL: {globals.longest_page['url']}\n"
    print_out += f"Longest page word count: {globals.longest_page['word_count']}\n"

    # Print top 50 words
    sorted_words = sorted(globals.word_frequencies.items(),
                            key=lambda item: item[1], reverse=True)
    top_50_words = sorted_words[:50]
    print_out += "Top 50 words:\n"
    for word, freq in top_50_words:
        print_out += f"{word}: {freq}\n"

    # Print subdomains
    sorted_subdomains = sorted(globals.subdomains.items())
    print_out += "Subdomains:\n"
    for subdomain, count in sorted_subdomains:
        print_out += f"{subdomain}, {count}\n"
        
    with open(filename, 'a') as f:    
        f.write(print_out)
        
    print("Data Saved!")



def scraper(url, resp):
    global unique_urls, longest_page, subdomains

    # Defragment the URL
    url, _ = urldefrag(url)

    # Check if the URL is unique

    if url in globals.unique_urls:
        return []

    should_evaluate_url = True
    if INCLUDE_URL_SIMILARITY_CHECKING:
        should_evaluate_url = link_similarity.go_thru_url_evaluation_phase_thread_safe(
            url)

    if not should_evaluate_url:
        return []

    globals.unique_urls.add(url)
    #save_data.update_unique_urls()

    # Update subdomains count
    parsed_url = urlparse(url)
    if "uci.edu" in parsed_url.netloc:
        subdomain = parsed_url.netloc.lower()
        globals.subdomains[subdomain] += 1

    should_go_thru_website: bool = True

    # if saving_count >= 100:
    #     saveFile()
    #     saving_count = 0

    # saving_count += 1
    # Process the page content if the response is valid
    if resp.status == 200 and resp.raw_response is not None:
        content_type = resp.raw_response.headers.get(
            'Content-Type', '').lower()
        if content_type and content_type.startswith('text/html'):
            try:
                # Parse the HTML content
                soup = BeautifulSoup(resp.raw_response.content, 'lxml')

                # Extract visible text from the page
                text = soup.get_text(separator=' ', strip=True)

                # Tokenize the text content
                tokens = tokenize(text)

                # Remove stop words from tokens
                filtered_tokens = filter_stop_words(tokens)

                if INCLUDE_N_GRAMS_PHASE:  # basically using this as a c pre processor command on whether or not to include the N_GRAMS_PHASE

                    should_go_thru_website = ngrams.go_thru_n_grams_phase_thread_safe(
                        filtered_tokens)

                if should_go_thru_website:
                    # Compute word frequencies
                    #globals.update_word_frequencies_thread_safe(filtered_tokens)

                    # Update longest page
                    word_count = len(filtered_tokens)
                    if word_count > globals.longest_page['word_count']:
                        globals.longest_page['word_count'] = word_count
                        globals.longest_page['url'] = url

                        # save_data.update_longest_page_wc(word_count)
                        # save_data.update_longest_page_url(url)

            except Exception as e:
                print(f"Error processing content from {url}: {e}")
    valid_links = list()

    if should_go_thru_website:

        # Extract next links
        links = extract_next_links(url, resp)

        # Filter links using is_valid
        valid_links = [link for link in links if is_valid(link)]

    return valid_links



# Testing purposes:
if __name__ == "__main__":
    # Print total unique pages
    
    test_str = '<html><head>\r\n<title>Sara on deck</title>\r\n</head>\r\n<body bgcolor=\"#ffffff\" text=\"#000000\">\r\n<div align=\"center\">\r\n<table width=\"95%\" cellspacing=\"5\">\r\n<tbody><tr><td align=\"left\" width=\"30%\"><a href=\"Darya2nd.html\">Prev: Darya reaches second</a></td>\r\n<td align=\"center\" width=\"30%\"><a href=\"index.html\">Up: Poison Ivy vs. Blue Angels</a></td>\r\n<td align=\"right\" width=\"30%\"><a href=\"CrystalCarrying3rd.html\">Next: Crystal helps pick up the bases</a></td>\r\n</tr></tbody></table>\r\n<h2>Sara on deck</h2>\r\n<img src=\"SaraOnDeck-m.jpg\" width=\"448\" height=\"672\" alt=\"Sara on deck\"><br><br>\r\n\r\n<h5>Taken Wednesday, April 23, 2003, 06:50:40pm.&nbsp; Original image size: 2048x3072, 5.5Mb<br>\r\nTechnical details: Canon EOS D60, 1/125s @ F4.0, ISO 100, 70-200mm/F2.8+1.4x @ 280mm (448mm equiv)<br>\r\nPS7 CRW 4700:-5, 0:10:40:45:0:25:5+M</h5>\r\n</div>\r\n</body></html>'
    
    if len(word_conversion(test_str)) <= 100:
        print("Early Exit: Low information")
    print(len(word_conversion(test_str)))
    print(word_conversion(test_str))
    saveFile()
    # print(f"Total unique pages: {len(globals.unique_urls)}")

    # # Print the longest page info
    # print(f"Longest page URL: {globals.longest_page['url']}")
    # print(f"Longest page word count: {globals.longest_page['word_count']}")

    # # Print top 50 words
    # sorted_words = sorted(globals.word_frequencies.items(),
    #                       key=lambda item: item[1], reverse=True)
    # top_50_words = sorted_words[:50]
    # print("Top 50 words:")
    # for word, freq in top_50_words:
    #     print(f"{word}: {freq}")

    # # Print subdomains
    # sorted_subdomains = sorted(globals.subdomains.items())
    # print("Subdomains:")
    # for subdomain, count in sorted_subdomains:
    #     print(f"{subdomain}, {count}")
