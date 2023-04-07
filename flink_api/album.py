import requests


def find_album_by_id(_id: int):
    url = f'https://jsonplaceholder.typicode.com/albums/{_id}'
    _resp = requests.get(url)
    if _resp.status_code == 200:
        return _resp.json()['title']
    else:
        return None


if __name__ == "__main__":
    response = find_album_by_id(1)
    print(response)
