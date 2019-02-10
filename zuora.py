import base64
import boto3
import os
from urllib import request
from urllib.parse import urlencode
import json


class Zuora():
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = self.zuora_token()

    def post(self, url, headers, data):
        req = request.Request(url, headers=headers,
                              data=data,
                              method='POST')
        with request.urlopen(req) as res:
            res_data = json.loads(res.read())
        return res_data

    def zuora_token(self):
        """Get zuora token.
        Returns:
            str: zuora token.
        """
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        url = 'https://rest.zuora.com/oauth/token'
        raw_data = {'grant_type': 'client_credentials',
                    'client_id': self.client_id,
                    'client_secret': self.client_secret}
        data = urlencode(raw_data).encode('utf-8')
        return self.post(url, headers, data)['access_token']

    def query_all(self, table, columns=['Id']):
        """Get all data.
        Args:
            table (str): table name.
            columns (:obj:`list` of :obj:`str`): column names list.

        Returns:
            list of selected data.
        """
        first_fetch = self.query(table, columns)
        records = first_fetch['records']
        query_locator = first_fetch.get('queryLocator')
        while query_locator:
            more_fetch = self.query_more(query_locator)
            records.extend(more_fetch['records'])
            query_locator = more_fetch.get('queryLocator')
        return records

    def query(self, table, columns=['Id']):
        """Fetch first 2,000 rows.
        Args:
            table (str): table name.
            columns (:obj:`list` of :obj:`str`): column names list.

        Returns:
            list of selected data.
        """
        columns = ','.join(columns)
        url = 'https://rest.zuora.com/v1/action/query'
        headers = {'Authorization': f'Bearer {self.token}',
                   'Content-Type': 'application/json'}
        json_data = {'queryString': f'SELECT {columns} FROM {table}'}
        data = json.dumps(json_data).encode('utf-8')
        return self.post(url, headers, data)

    def query_more(self, query_locator):
        """Fetch first 2,000 rows from offset.
        Args:
            query_locator (str): where query starts.

        Returns:
            list of selected data.
        """
        url = 'https://rest.zuora.com/v1/action/queryMore'
        headers = {'Authorization': f'Bearer {self.token}',
                   'Content-Type': 'application/json'}
        data = json.dumps({'queryLocator': query_locator}).encode('utf-8')
        return self.post(url, headers, data)

    def subscriptions(self, columns=['Id']):
        """Get subscription data.
        Args:
            columns (:obj:`list` of :obj:`str`): column names list.

        Returns:
            list of selected data.
        """
        return self.query_all(table='Subscription', columns=columns)


def kms_decrypt(encrypted_txt):
    ciphertext_blob = base64.b64decode(encrypted_txt)
    kms = boto3.client('kms')
    dec = kms.decrypt(CiphertextBlob=ciphertext_blob)
    return dec['Plaintext'].decode('utf-8')


if __name__ == '__main__':
    zuora = Zuora(client_id=os.environ['client_id'],
                  client_secret=os.environ['client_secret'])
    print(len(zuora.subscriptions(columns=['AccountId'])))
