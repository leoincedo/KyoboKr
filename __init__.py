#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:fdm=marker:ai
from __future__ import absolute_import, division, print_function, unicode_literals

__license__ = 'GPL v3'
__copyright__ = '2024, leoincedo based on 2021, YoungJae Hur <yjhur82 at gmail.com> based on google search by Kovid Goyal <kovid at kovidgoyal.net>'
__docformat__ = 'restructuredtext en'

import time, re, os
from threading import Thread
from lxml.html import fromstring

try:
    from queue import Empty, Queue
except ImportError:
    from Queue import Empty, Queue

from calibre import as_unicode, random_user_agent
from calibre.ebooks.metadata import check_isbn
from calibre.ebooks.metadata.sources.base import Source
from urllib.parse import urlparse
from calibre.utils.localization import canonicalize_lang, get_lang
from polyglot.builtins import iteritems, cmp

import difflib

# Comparing Metadata objects for relevance {{{
words = ("the", "a", "an", "of", "and")
prefix_pat = re.compile(r'^(%s)\s+'%("|".join(words)))
trailing_paren_pat = re.compile(r'\(.*\)$')
whitespace_pat = re.compile(r'\s+')

def get_series_info(title, strings):
    #print('****** > find_prefix@0',strings)
    if not strings:
        return ""

    #print('****** > find_prefix@1')
    results = sorted(strings, key=lambda x: difflib.SequenceMatcher(None, x.split(":")[0], title).ratio(), reverse=True)

    if len(results) <= 0:
        return ""

    name = results[0]
    name = name.split(":")[0].strip()
    name = name.replace("("," ").replace(")","")
    sp = name.strip().split(" ")

    isSplit = True 
    end = sp[-1]
    try:
        x = int(end)
    except:
        isSplit = False

    name = ""
    index = 0

    print("DEBUG 0")

    series_index = re.findall("\s+(?:\d*\.*\d+)\s*", title)
    if series_index:
        print("DEBUG 1", series_index)
        index = float(series_index[0].strip())
   
    print("DEBUG :", sp, end) 
    if end == '상' or end =='중' or end =='하':
        isSplit = True
    
    if end == "상":
        index = 1.0
    if end == "중":
        index = 2.0
    if end == "하":
        index = 3.0

    if isSplit:
        name = " ".join(sp[0:-1])
    else:
        name = " ".join(sp)
    
    return (name, index)


def cleanup_title(s):
    if not s:
        s = _('Unknown')
    s = s.strip().lower()
    s = prefix_pat.sub(' ', s)
    s = trailing_paren_pat.sub('', s)
    s = whitespace_pat.sub(' ', s)
    return s.strip()

def check_date_components_in_string(s):
    return '년' in s and '월' in s and '일' in s

def getItemID(url):
    parsed_url = urlparse(url)

    path_segments = parsed_url.path.split('/')
    last_segment = path_segments[-1]  # 리스트의 마지막 요소
    return last_segment

class Worker(Thread):  # {{{

    def __init__(self, basic_data, relevance, result_queue, br, timeout, log, plugin):
        Thread.__init__(self)
        self.daemon = True
        self.br, self.log, self.timeout = br, log, timeout
        self.result_queue, self.plugin, self.kyobo = result_queue, plugin, basic_data['kyobo']
        self.relevance = relevance

    def run(self):
        url = "https://product.kyobobook.co.kr/detail/{}".format(self.kyobo)
        if self.kyobo[0] == 'E':
            url = "https://ebook-product.kyobobook.co.kr/dig/epd/ebook/{}".format(self.kyobo)

        self.log('WORK RUN ', url)
        try:
            mi = self.parseItemPage(url)
            if mi != None:
                mi.source_relevance = self.relevance * 100
                self.plugin.clean_downloaded_metadata(mi)
                self.result_queue.put(mi)
        except:
            self.log.exception('Failed to parse details for kyobo: {}'.format(self.kyobo))
        #self.log('WORK END ', url, 'QUEUE: ', self.result_queue.qsize())

    def to_str(self, bytes_or_str):
        if isinstance(bytes_or_str, bytes):
            value = bytes_or_str.decode('utf-8')
        else:
            value = bytes_or_str
        return value

    def getComment(self, ref, isbn):
        return None

        try:
            from urllib.parse import urlencode
        except ImportError:
            from urllib import urlencode

        comment_list = []

        comment_base_url = "https://www.aladin.co.kr/shop/product/getContents.aspx?"
        aladin_comment_url = comment_base_url + urlencode(dict(ISBN=isbn, name='Introduce'))
        publisher_comment_url = comment_base_url + urlencode(dict(ISBN=isbn, name='PublisherDesc'))
        aladin_comment = self.parseComment(ref, aladin_comment_url)
        publisher_comment = self.parseComment(ref, publisher_comment_url)

        if aladin_comment:
            comment_list.append("책소개")
            comment_list.append(str(aladin_comment).strip())

        if publisher_comment:
            comment_list.append("출판사 책소개")
            publisher_commnet = re.sub(" 접기$", "", str(publisher_comment).strip())
            comment_list.append(publisher_commnet.strip())

        comment = "\n\n".join(comment_list)
        comment = re.sub("\r", "", comment)
        return comment

        # if comment_list:
        #     comment = "\n\n".join(comment_list)
        #     comment = re.sub("\r", "", comment)
        #     return comment
        #
        # else:
        #     return None

    def parseComment(self, ref, url):
        comment = ''
        return comment

        br = self.br.clone_browser()
        br.addheaders = [
            ('Referer', ref),
        ]
        raw = br.open_novisit(url, timeout=self.timeout).read()
        try:
            html = fromstring(raw.decode('utf-8'))
        except:
            self.log('Comment page empty', url)
            return comment
        for comment_node in html.xpath("//div[contains(@class, 'Ere_prod_mconts_LS') and contains(text(),'책소개')]"):
            full_length = comment_node.xpath("..//div[@id='div_PublisherDesc_All']")
            if full_length:
                comment = full_length[0].text_content()
            else:
                comment = comment_node.xpath("../div[contains(@class, 'Ere_prod_mconts_R')]")[0].text_content()
        return comment

    def parseItemPage(self, url):
        try:
            raw = self.br.open_novisit(url, timeout=self.timeout).read().decode('utf-8')
            html = fromstring(raw)
            if not html.xpath("//span[@class='prod_title']"):
                if '19세' in raw:
                    self.log.warning('19세 연령제한 페이지입니다.')
                    home = os.path.expanduser('~')
                    desktop = os.path.join(home, 'Desktop')
                    file = os.path.join(desktop, self.kyobo + '.html')
                    try:
                        raw = open(file).read()
                    except:
                        self.log.error(file, 'not found')
                    html = fromstring(raw)
                    self.log.debug('loaded saved page')
            else:
                self.log.debug('ItemQuery ', url)
        except:
            self.log.exception('Failed to load item page: %r' % url)
            return
        
        #raw = raw.decode('utf-8')

        #f = open('\\workspace\out2.txt','r')
        #f.write(str(raw.decode('utf-8')))
        #raw = f.read() 
        #f.close()

        

        print('parseItemPage', self.kyobo)
        mi = self.getMetaInstance()
        mi.set_identifier('kyobo', self.kyobo)

        xhtml = fromstring(raw)
        elems = xhtml.xpath("//span[@class='prod_title']")
        title = elems[0].text
        elems = xhtml.xpath("*//div[@class='author']//text()")
        elems = [s.strip() for s in elems]
        authors = elems[0:-1]
        authors = filtered_list = [item for item in authors if item != '' and item != '>']

        #author = " ".join(elems)
        #author = author.strip()
        
        elems = xhtml.xpath("//div[@class='prod_info_text publish_date']//text()")
        elems = [s.strip() for s in elems]
        elems = [item for item in elems if item != '' and item != '>']
        publisher = elems[0]
        elems = [s for s in elems[1:] if check_date_components_in_string(s)]
        pubdate = elems[0].replace("·","",1).strip()
        cleaned_str = pubdate.replace('\n', '').replace(' ', '')
        pubdate = cleaned_str.split('출간')[0]

        print('pubdate:', pubdate)
        elems = xhtml.xpath("//li[@class='category_list_item']//text()")
        categories = [s.strip() for s in elems]
        filtered_list = [item for item in categories if item != '' and item != '>']
        categories = list(dict.fromkeys(filtered_list))
        desc = ""        
        elems = xhtml.xpath("//div[@class='intro_bottom']//div[@class='info_text']/text()")
        if len(elems) > 0 :
            desc = " ".join(elems)

	
        elems = xhtml.xpath("//input[@class='form_rating']/@value")
        rating = 0

        if len(elems) > 0:
            rating = elems[0]

        print('rating : ', rating)

        elem = xhtml.xpath("""//div[@class="portrait_img_box"]/img/@src""")


        if len(elem):
            img_src = elem[0]
            mi.has_cover = self.plugin.cache_identifier_to_cover_url(self.kyobo, img_src)  is not None

        mi.authors = []
        for o in authors :
            mi.authors.append(o)

        elem = xhtml.xpath('//tr[th[.="ISBN"]]/td/text()')
        if len(elem) == 0 :
            elem = xhtml.xpath('//*[@class="prod_pordInfo_box indent"]/dd[2]/em/text()')

        if len(elem) > 0:
            isbn = elem[0] 
            mi.set_identifier('isbn', isbn )
            mi.isbn = isbn


        mi.comments = desc
        mi.publisher = publisher
        mi.title = title
        from calibre.utils.date import parse_only_date
        from datetime import datetime

        date_obj = datetime.strptime(pubdate, "%Y년%m월%d일")
        formatted_date = date_obj.strftime("%Y/%m/%d")
        mi.pubdate = parse_only_date(formatted_date)

        for tag in categories:
            mi.tags.append(tag)

        mi.rating = float(rating) / 2
        
        query ="https://product.kyobobook.co.kr/api/gw/pdt/product/{}/series?per=20".format(self.kyobo)

        try:
            raw = self.br.clone_browser().open(query, timeout=3).read().decode('utf-8')
        except Exception as e:
            return as_unicode(e)
        import json

        try:
            json_data = json.loads(raw)
            names_list = [item['name'] for item in json_data['data']['list']]

            print(names_list, len(names_list))
            if len(names_list) > 0:
                (name, index) = get_series_info(title, names_list)
                print('name , ', name)
                if len(name) > 0:
                    mi.series = name
                if index > 0:
                    mi.series_index = index
        except:
            pass

        print("SERIES :", mi.series, mi.series_index)

        #print(mi) 

        return mi

    def getMetaInstance(self):
        from calibre.ebooks.metadata.book.base import Metadata
        from calibre.utils.date import UNDEFINED_DATE

        mi = Metadata(title=_('Unknown'))
        mi.authors = []
        mi.pubdate = UNDEFINED_DATE
        mi.tags = []
        mi.languages = ["Korean"]
        return mi


class KyoboKr(Source):
    name = 'KyoboKr'
    author = 'leoincedo'
    version = (1, 0, 4)
    minimum_calibre_version = (3, 6, 0)
    description = _('교보에서 책 정보와 표지 다운로드 - AladinKr Project 참조')

    capabilities = frozenset(['identify', 'cover'])
    touched_fields = frozenset([
        'title', 'authors', 'tags', 'pubdate', 'comments', 'publisher',
        'identifier:isbn', 'identifier:kyobo', 'identifier:kyobobook.co.kr',
        'rating'])
    supports_gzip_transfer_encoding = True
    has_html_comments = False
    log = None

    @property
    def user_agent(self):
        # Pass in an index to random_user_agent() to test with a particular
        # user agent
        try:
            return random_user_agent(allow_ie=False)
        finally:
            print('user_agent', self)

    def _get_book_url(self, args):
        print('_get_book_url')
        url = "https://product.kyobobook.co.kr/detail/{}".format(args)
        if self.kyobo[0] == 'E':
            url = "https://ebook-product.kyobobook.co.kr/dig/epd/ebook/{}".format(args)
        return url

    def get_book_url(self, identifiers):  # {{{
        print('get_book_url')
        if identifiers.get('kyobo', None):
            args = identifiers.get('kyobo', None)
            return 'kyobo', args, self._get_book_url(args)
        if identifiers.get('kyobobook.co.kr', None):
            args = identifiers.get('kyobobook.co.kr', None)
            return 'kyobo', args, self._get_book_url(args)

    # }}}

    def get_cached_cover_url(self, identifiers):  # {{{
        print('get_cached_cover_url')
        sku = None
        if identifiers.get('kyobo', None):
            sku = identifiers.get('kyobo', None)
        elif identifiers.get('kyobobook.co.kr', None):
            sku = identifiers.get('kyobobook.co.kr', None)
        elif identifiers.get('isbn', None):
            isbn = identifiers.get('isbn', None)
            sku = self.cached_isbn_to_identifier(isbn)
        return self.cached_identifier_to_cover_url(sku)

    # }}}

    def replace_number_at_end(self, text):
        # 문자열 끝에 오는 숫자 부분을 찾습니다.
        match = re.search(r'(\d+)(권)?$', text)

        if match:
            # 추출한 숫자를 가져옵니다.
            number = int(match.group(1))
            # "권"이 포함되어 있다면 "권"을 제거합니다.
            if match.group(2) == "권":
                text = text[:-1]  # "권"을 제외하고 다시 연결합니다.
            # 숫자를 그대로 사용하여 치환합니다.
            replaced_text = re.sub(match.group(1), str(number), text)
            return replaced_text
        else:
            # 숫자가 없는 경우에는 원래 문자열을 반환합니다.
            return text

    def create_query(self, log, title=None, authors=None, identifiers={}):
        print('query')
        try:
            from urllib.parse import urlencode
        except ImportError:
            from urllib import urlencode

        self.log = log 
        log.info('CREATE_QUERY @2', identifiers)
        BASE_URL = "https://search.kyobobook.co.kr/search?"
        params = {
            #'ViewRowCount': 50,  # 50 results are the maximum
        }
        #isbn = None
        isbn = check_isbn(identifiers.get('isbn', None))
        log.info('CREATE_QUERY @2', title, isbn)
        if isbn:
            params['KeyISBN'] = isbn
            return BASE_URL + urlencode(params)
        elif title or authors:
            log.info('CREATE_QUERY@2')
            params['keyword'] = []
            title2 = self.replace_number_at_end(title)
            if authors != None:
                title2 = title2 + " " + authors[0]
            """title_tokens = list(self.get_title_tokens(title2))

            if title_tokens:
                params['keyword'].extend(title_tokens)"""
            log.info('debug :' + title2)
            log.info(params['keyword'])

            # author_tokens = self.get_author_tokens(authors, only_first_author=True)

            # if author_tokens:
            #    params['SearchWord'].extend(author_tokens)
            params['keyword'] = title2
            #params['keyword'] = ' '.join(params['keyword'])
            #print(params)
            return BASE_URL + urlencode(params)
        else:
            return None

    # }}}

    def identify_results_keygen(self, title, authors, identifiers ):

        print('@@@@@keygen : ', identifiers)
        def keygen(mi):
            return InternalMetadataCompareKeyGen(mi, self, title, authors,
                identifiers)
        return keygen


    def parseList(self, raw, log, keyword=''):
        xhtml = fromstring(raw)
        print('parselist', xhtml)
        print('parselist@@@@@@@@@@@@@@', flush=True)
        
        items = []
        sorted_books = []
        for prod_item in xhtml.xpath("//li[@class='prod_item']"):
            #print(prod_item.text)
            title = prod_item.xpath(".//span[contains(@id, 'cmdtName')]")
            hrefs = prod_item.xpath('.//a/@href')
            item = {}

            item['title'] = title[0].text
            item['url'] = hrefs[0]
            item['score'] = 0
            item['itemId'] = getItemID( hrefs[0] )

            answer_bytes = bytes(keyword, 'utf-8')
            input_bytes = bytes(item['title'], 'utf-8')
            answer_bytes_list = list(answer_bytes)
            input_bytes_list = list(input_bytes)

            sm = difflib.SequenceMatcher(None, answer_bytes_list, input_bytes_list)
            similar = sm.ratio()
            item['score'] = similar
            items.append(item)

            sorted_books = sorted(items, key=lambda x: x['score'], reverse=True)

        log.info('sorted : ',sorted_books)


        return [dict(kyobo=x['itemId']) for x in sorted_books]
        

    def identify(self, log, result_queue, abort, title=None, authors=None,  # {{{
                 identifiers={}, timeout=30):

        print('iidentify', result_queue)
        br = self.browser
        br.addheaders = [
            ('Referer', 'https://search.kyobobook.co.kr/'),
            #('X-Requested-With', 'XMLHttpRequest'),
            #('Cache-Control', 'no-cache'),
            #('Pragma', 'no-cache'),
            #('verify_ssl', 'True'),
            ("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) "
                           "Version/14.1 Safari/605.1.15"),
        ]

        if 'kyobo' in identifiers:
            items = [dict(kyobo=identifiers['kyobo'])]
        elif 'kyobobook.co.kr' in identifiers:
            items = [dict(kyobo=identifiers['kyobobook.co.kr'])]
        else:
            log.info('hey', title)
            query = self.create_query(log, title=title, authors=authors,
                                      identifiers=identifiers)
            if not query:
                log.error('Insufficient metadata to construct query')
                return
            log('Using query URL@1:', query)
            try:
                raw = br.open(query, timeout=timeout).read().decode('utf-8')
            except Exception as e:
                log.exception('Failed to make identify query: %r' % query)
                return as_unicode(e)

            #f = open('\\workspace\\out.txt', "r")
            #raw = f.read()
            #f.close()
            items = self.parseList(raw, log, title)
            print('parseList', items)

            if len(items) == 0:
                sp = title.split(" ")
                title2 = " ".join(sp[1:3])
                #title2 = title2 + " " + authors
                return self.identify(log, result_queue, abort, title=title2, authors=authors, timeout=timeout)

            if items is None:
                log.error('Failed to get list of matching items')
                log.debug('Response text:')
                log.debug(raw)
                return


        if (not items and identifiers and title and authors and
                not abort.is_set()):
            return self.identify(log, result_queue, abort, title=title, authors=authors, timeout=timeout)
        if not items:
            return


        newQ = Queue() 
        workers = []
        for i, item in enumerate(items):
            print('@@@Work', i, item)
            workers.append(Worker(item, i, newQ , br.clone_browser(), timeout, log, self))

        if not workers:
            return

        for w in workers:
            w.start()
            # Don't send all requests at the same time
            time.sleep(0.1)

        #for w in workers:
        #    w.join(5)

        while not abort.is_set():
            a_worker_is_alive = False
            for w in workers:
                w.join(0.2)
                if abort.is_set():
                    break
                if w.is_alive():
                    a_worker_is_alive = True
            if not a_worker_is_alive:
                break

        books = []
        items = []
        sorted_books = []

        #self.log('QUEUE ', len(workers), newQ.qsize(), result_queue.qsize())

        while(newQ.qsize() > 0):
            books.append( newQ.get() )

        for item in books: 
            answer_bytes = title
            input_bytes = item.title
            #answer_bytes_list = list(answer_bytes)
            #input_bytes_list = list(input_bytes)

            #maxlen = int(abs(len(answer_bytes_list) + len(input_bytes_list)) / 2)
            #maxlen = int(len(answer_bytes_list)) + 1


            input_string = input_bytes
            if ":" in item.title:
                input_string = str(input_bytes).split(':')[0]
            check_len = len(answer_bytes) + 3

            sm = difflib.SequenceMatcher(None, answer_bytes, input_string[:check_len], autojunk=False)
            similar = sm.ratio()
            item.source_relevance = similar  * 100
            items.append({'item':item, 'score': similar * 100})
            #self.log('ITEM SCORE : ', item.title, item.source_relevance )

        sorted_books = sorted(items, key=lambda x: x['score'], reverse=True)

        #self.log('sorted books @2: ',sorted_books )

        for b in sorted_books:
            result_queue.put(b['item'])

        print('result_queue : ', result_queue.qsize())
    # }}}

    def download_cover(self, log, result_queue, abort,  # {{{
                       title=None, authors=None, identifiers={}, timeout=30, get_best_cover=False):
        cached_url = self.get_cached_cover_url(identifiers)
        if cached_url is None:
            log.info('No cached cover found, running identify')
            rq = Queue()
            self.identify(log, rq, abort, title=title, authors=authors,
                          identifiers=identifiers)
            if abort.is_set():
                return
            results = []
            while True:
                try:
                    results.append(rq.get_nowait())
                except Empty:
                    break
            results.sort(key=self.identify_results_keygen(
                title=title, authors=authors, identifiers=identifiers))
            for mi in results:
                cached_url = self.get_cached_cover_url(mi.identifiers)
                if cached_url is not None:
                    break
        if cached_url is None:
            log.info('No cover found')
            return

        if abort.is_set():
            return
        br = self.browser
        log('Downloading cover from:', cached_url)
        try:
            cdata = br.open_novisit(cached_url, timeout=timeout).read()
            result_queue.put((self, cdata))
        except:
            log.exception('Failed to download cover from:', cached_url)
    # }}}

class InternalMetadataCompareKeyGen:

    '''
    Generate a sort key for comparison of the relevance of Metadata objects,
    given a search query. This is used only to compare results from the same
    metadata source, not across different sources.

    The sort key ensures that an ascending order sort is a sort by order of
    decreasing relevance.

    The algorithm is:

        * Prefer results that have at least one identifier the same as for the query
        * Prefer results with a cached cover URL
        * Prefer results with all available fields filled in
        * Prefer results with the same language as the current user interface language
        * Prefer results that are an exact title match to the query
        * Prefer results with longer comments (greater than 10% longer)
        * Use the relevance of the result as reported by the metadata source's search
           engine
    '''

    def __init__(self, mi, source_plugin, title, authors, identifiers):
        same_identifier = 2
        idents = mi.get_identifiers()
        for k, v in iteritems(identifiers):
            if idents.get(k) == v:
                same_identifier = 1
                break

        all_fields = 1 if source_plugin.test_fields(mi) is None else 2

        exact_title = 1 if title and \
                cleanup_title(title) == cleanup_title(mi.title) else 2

        language = 1
        if mi.language:
            mil = canonicalize_lang(mi.language)
            if mil != 'und' and mil != canonicalize_lang(get_lang()):
                language = 2

        has_cover = 2 if (not source_plugin.cached_cover_url_is_reliable or
                source_plugin.get_cached_cover_url(mi.identifiers) is None) else 1

        print('####self.base ', mi.title, mi.identifiers, mi.source_relevance)
        self.base = (same_identifier, has_cover, all_fields, language, exact_title)
        self.comments_len = len((mi.comments or '').strip())
        self.extra = getattr(mi, 'source_relevance', 0)

    def compare_to_other(self, other):
        try:
            if self.extra != other.extra:
                print('extra, ', self.base, self.extra, other.base, other.extra )
                return other.extra - self.extra
                #return self.extra - other.extra
            else:
                print('extra@2, ', self.base, self.extra, other.base, other.extra )
        except:
            pass
        return 0
                
        a = cmp(self.base, other.base)
        if a != 0:
            return a
        cx, cy = self.comments_len, other.comments_len
        if cx and cy:
            t = (cx + cy) / 20
            delta = cy - cx
            if abs(delta) > t:
                return -1 if delta < 0 else 1

    def __eq__(self, other):
        return self.compare_to_other(other) == 0

    def __ne__(self, other):
        return self.compare_to_other(other) != 0

    def __lt__(self, other):
        return self.compare_to_other(other) < 0

    def __le__(self, other):
        return self.compare_to_other(other) <= 0

    def __gt__(self, other):
        return self.compare_to_other(other) > 0

    def __ge__(self, other):
        return self.compare_to_other(other) >= 0

# }}}

if __name__ == '__main__':
    from calibre.ebooks.metadata.sources.test import (
        test_identify_plugin, title_test, authors_test, comments_test, pubdate_test, series_test)

    print('test', KyoboKr.name)
    tests = [
        # (  # A book with an ISBN
        #     {'identifiers': {'isbn': '9788939205109'}},
        #     [title_test('체 게바라 평전', exact=True),
        #     authors_test(['장 코르미에','김미선']),
        #     ]
        # ),
        (  # A book with an aladin id
            {'identifiers':{}, 'title':'귀멸의칼날'},
            [title_test('귀멸의칼날', exact=False)]
        ),
        # (  # A book with an aladin id
        #     {'identifiers': {'aladin': '208556'}},
        #     [title_test('Harry', exact=False)]
        # ),
    ]
    start, stop = 0, len(tests)

    tests = tests[start:stop]
    test_identify_plugin(KyoboKr.name, tests)

