import re
from urllib.parse import parse_qs, urldefrag, urljoin, urlparse
from bs4 import BeautifulSoup
from collections import defaultdict, Counter

DOKU_MEDIA_PARAMS = {"do", "tab_files", "tab_details", "image", "ns"}

STOPWORDS = ["a", "about", "above", "after", "again", "against", "all", "am", "an", "and",
    "any", "are", "aren't", "as", "at", "be", "because", "been", "before",
    "being", "below", "between", "both", "but", "by", "can't", "cannot", "could",
    "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't",
    "down", "during", "each", "few", "for", "from", "further", "had", "hadn't",
    "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's",
    "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how",
    "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is",
    "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most",
    "mustn't", "my", "myself", "no", "nor", "not", "of", "off", "on", "once",
    "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over",
    "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should",
    "shouldn't", "so", "some", "such", "than", "that", "that's", "the", "their",
    "theirs", "them", "themselves", "then", "there", "there's", "these", "they",
    "they'd", "they'll", "they're", "they've", "this", "those", "through", "to",
    "too", "under", "until", "up", "very", "was", "wasn't", "we", "we'd",
    "we'll", "we're", "we've", "were", "weren't", "what", "what's", "when",
    "when's", "where", "where's", "which", "while", "who", "who's", "whom",
    "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd",
    "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"]

SUBDOMAIN_PAGE_COUNT = defaultdict(set) # q4
CRAWLED_CONTENT_HASHES = set() # duplication searc
WORD_FREQUENCIES = Counter() # q3
TOTAL_UNIQUE_PAGES = set() # q1
LONGEST_PAGE = {"url": None, "word_count": 0} # q2

MAX_SIZE = 500_000
NEAR_DUPLICATE = set()


def scraper(url, resp):
    links = extract_next_links(url, resp)
    valid_links = [link for link in links if is_valid(link)]
    
    for link in valid_links:
        parsed = urlparse(link)
        host = parsed.hostname.lower()
        if host.endswith(".uci.edu"):
            SUBDOMAIN_PAGE_COUNT[host].add(link)

    return valid_links

def tokenize(text: str) -> list[str]:
    text = text.lower()
    tokens = re.findall(r'\b[a-zA-Z]{2,}\b', text)

    return tokens

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    
    links = []

    # basic check
    if resp is None or resp.status != 200 or resp.raw_response is None:
        return links

    # only want html pages
    c_type = (resp.raw_response.headers.get('Content-Type') or '').lower()
    if 'text/html' not in c_type:
        return links

    if resp.raw_response and is_duplicate(resp.raw_response.content):
        return []

    # get the html 
    html = resp.raw_response.content
    soup = BeautifulSoup(html, 'lxml')

    # count words for Question 2
    text = soup.get_text(separator=' ')
    words = [w for w in tokenize(text)] # if w and w not in STOPWORDS => this is so that we can count words that ARE stop words to measure pages with largest word count. need to implement the stopwords for q3
    if is_near_duplicate(words):
        return links
    word_count = len(words)

    if word_count < 50:
        return links
    elif word_count < 300 and len(html) > MAX_SIZE:
        return links

    TOTAL_UNIQUE_PAGES.add(url)
    WORD_FREQUENCIES.update(words)

    if word_count > LONGEST_PAGE['word_count']:
        LONGEST_PAGE["url"] = url
        LONGEST_PAGE["word_count"] = word_count

    # extract all anchor tags
    for a_tag in soup.find_all('a', href=True):
        raw = a_tag['href'].strip()
        # no non-web linkts
        if raw.startswith('mailto:') or raw.startswith('javascript:') or raw.startswith('tel:'):
            continue

        # clean up url and then add to list
        try:
            absolute_url = urljoin(resp.raw_response.url or url, raw)
        except ValueError:
            # skip malformed links like http://YOUR_IP/
            continue
        clean_url, _ = urldefrag(absolute_url)  # remove fragment
        links.append(clean_url)


    # remove any duplicates and return list
    unique_pages = set(links) #saved into var so we can calculate the length of the list
    return list(unique_pages)
    

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    allowed_domains = ( ".ics.uci.edu", ".cs.uci.edu", ".informatics.uci.edu", ".stat.uci.edu")

    try:
        parsed = urlparse(url)
        # scheme check
        if parsed.scheme not in {"http", "https"}:
            return False
        host = (parsed.hostname or "").lower()

        # got stuck in a spider trap so this should help 
        if parsed.fragment:
            return False
        if "timeline" in parsed.path.lower() or re.search(r"/\d{4}/\d{2}/\d{2}", parsed.path) or re.search(r"date=\d{4}-\d{2}-\d{2}", parsed.query):
            return False
        
        if (
            "/events/" in parsed.path
            or "ical" in parsed.path
            or "tribe" in parsed.path
            or "/ca/rules" in parsed.path
        ):
            return False
        
        if host == "gitlab.ics.uci.edu":
            return False

        if not (
            any(host.endswith(domain) for domain in allowed_domains)
            or (host.endswith("today.uci.edu") and parsed.path.startswith("/department/information_computer_sciences"))
        ):
            return False

        if not valid_query(parsed.query):
            return False
        
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise
        
def valid_query(parsed):
    q = parse_qs(parsed.query or "")
    
    if ("do" in q and "media" in q["do"]):
            return False
    if any(k in DOKU_MEDIA_PARAMS for k in q.keys()):
        return False

    # too many query params
    if len(q) > 100:
        return False

    return True

def is_duplicate(content):
    content_hash = compute_content_hash(content)
    if content_hash in CRAWLED_CONTENT_HASHES:
        return True # already seen
    else:
        CRAWLED_CONTENT_HASHES.add(content_hash)
        return False # never seen 

def compute_content_hash(content): 
    # converts html bytes --> hashable string hash 
    text = content.decode('utf-8', errors='ignore')
    return polynomial_rolling_hash(text)

def polynomial_rolling_hash(s, base=31, mod=10**9 + 9):
    # Used Geeks for Geeks as a reference: 
    #   https://www.geeksforgeeks.org/dsa/string-hashing-using-polynomial-rolling-hash-function/ 
    
    # converts into hash 
    hash_value = 0
    power = 1
    for ch in s.lower():
        if 'a' <= ch <= 'z':
            hash_value = (hash_value + (ord(ch) - ord('a') + 1) * power) % mod
        else:
            hash_value = (hash_value + ord(ch) * power) % mod
        power = (power * base) % mod
    return hash_value
    
def is_near_duplicate(tokens) -> bool:
    similarity_threshold = 0.85
    min_token_count = 10
    if len(tokens) < min_token_count:
        return False

    trigrams = [' '.join(tokens[i:i + 3]) for i in range(len(tokens) - 2)]

    trigram_hashes = {polynomial_rolling_hash(ngram) for ngram in trigrams}

    selected_hashes = {h for h in trigram_hashes if h % 4 == 0}

    for fingerprint in NEAR_DUPLICATE:
        intersection = selected_hashes.intersection(fingerprint)
        union = selected_hashes.union(fingerprint)
        similarity_score = len(intersection) / len(union) if union else 0.0
        if similarity_score >= similarity_threshold:
            return True

    NEAR_DUPLICATE.add(frozenset(selected_hashes))
    return False

def generate_report(filename="report.txt"):
    sw = set(STOPWORDS)
    filtered = Counter({word: count for word, count in WORD_FREQUENCIES.items() if word not in sw})

    with open(filename, "w") as file:
        file.write(f"Unique pages: {len(TOTAL_UNIQUE_PAGES)}\n")
        file.write(f"Longest word count url: {LONGEST_PAGE}\n")
        file.write(f"Longest word count: {LONGEST_PAGE['word_count']}\n")

        for word, count in filtered.most_common(50):
            file.write(f"{word}: {count}\n")

        all_subdomains = sorted(SUBDOMAIN_PAGE_COUNT.keys())
        file.write(f'Total subdomains: {len(all_subdomains)}\n')
        
        for subdomain in all_subdomains:
            file.write(f"{subdomain}: {len(SUBDOMAIN_PAGE_COUNT[subdomain])}\n")