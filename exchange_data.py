import bs4
import requests
import os
import zipfile
import tqdm
from datetime import datetime


def get_data(output, year=0):
    """
    分足のデータをwww.forexite.comから取得して保存する。
    データに記載されている時刻はGMT+1。
    :param output: 保存先ディレクトリパス
    :param year: -1->全て、0->最新のみ、1以降->年数(2年以上10年以下)
    :return:
    """
    if not os.path.exists(output):
        os.mkdir(output)

    # urlの配列を受け取り
    print("データの配置場所のURLを収集中。")
    urls = collect_url(year)
    print("収集完了。")

    # ダウンロードしてきたファイルの置き場所
    out_file_path_ary = []

    print("データを取得中。")
    for data_url in tqdm.tqdm(urls):
        file_name = __get_name(data_url)

        res = requests.get(data_url)
        dl_file_path = os.path.join(output, file_name)

        # ファイル取得と保存
        with open(dl_file_path, 'wb') as fp:
            fp.write(res.content)

        # 解凍
        with zipfile.ZipFile(dl_file_path) as zf:
            nl = zf.namelist()
            if len(nl) == 0:
                print("{0} を解凍しましたが、中身がありません。", {data_url})
                continue
            elif 1 < len(nl):
                print("{0} を解凍したところ、中身が複数あります。先頭のファイルのみ取得し、残りを破棄します。", {data_url})

            # 解凍したら1個しかファイルが無いはず
            name = zf.namelist()[0]

            # ダウンロード時に決定した拡張子以外のファイル名と
            # zipファイル内のファイル名の拡張子を組み合わせて使用する。
            fname, ext = os.path.splitext(dl_file_path)
            fname2, ext2 = os.path.splitext(name)
            unzip_path = fname + ext2

            with open(unzip_path, 'wb') as fp:
                fp.write(zf.read(name))

        os.remove(dl_file_path)

    print("処理完了。")


def __get_name(data_url):
    """
    ダウンロードして得られるzipの中身のファイル名がダメダメなので
    data_urlから解凍後に付けるファイル名を決定し、保存するzip名に使用する。
    解凍後は拡張子だけcsvにしたファイル名とする。

    param https://www.forexite.com/free_forex_quotes/2015/01/020115.zip
    result 201501020115.zip

    :param data_url:zipのurl。ex: https://www.forexite.com/free_forex_quotes/2015/01/020115.zip
    :return:
    """
    name = "".join(data_url.split('/')[-3:])

    return name


def collect_url(year):
    """
    zip配置場所のURLを返す。
    :param year: -1->全て、0->最新のみ、1以降->年数(2年以上10年以下)
    :return:
    """

    urls = []

    this_year = datetime.today().year

    # csvへの相対リンクが記載してあるページ
    url = 'https://www.forexite.com/free_forex_quotes/forex_history_arhiv.html'
    urls.append(url)

    suffix_list = []

    if year == -1:
        for y in range(2001, this_year-1):
            suffix_list.append("_{0}".format(y))
    elif year == 0:
        pass
    elif 1 < year <= 10:
        for y in range(this_year-year, this_year-1):
            suffix_list.append("_{0}".format(y))
    else:
        print("引数yearが不正です。{0}", {year})

    b, ext = os.path.splitext(url)
    for suffix in suffix_list:
        urls.append(b + suffix + ext)

    data_urls = []
    for url in urls:
        # base_url + 相対リンク でcsv取得先のURLになる。
        base_url = url[0: url.rfind("/") + 1]

        try:
            res = requests.get(url)
            soup = bs4.BeautifulSoup(res.text, "lxml")
            data_urls += [base_url + x.attrs['href'] for x in soup.find_all("a") if x.attrs['href'].endswith("zip")]
        except:
            print("{0} の解析に失敗。", {url})
            continue

    return data_urls


if __name__ == "__main__":
    get_data(output="D:\\tmp", year=-1)

    #print(__get_name("https://www.forexite.com/free_forex_quotes/2015/01/020115.zip"))
    #[print(x) for x in collect_url(-1)]
