import boto3
from boto3.dynamodb.conditions import Key


def get_item():
    print("get_item ...")
    client = boto3.resource('dynamodb')
    table = client.Table('chentest')

    response = table.get_item(
        Key={
            'id': 3,
            'name': "Grace"
        }
    )
    print(response)
    print(response['Item'])


def query():
    print("query ...")
    client = boto3.resource('dynamodb')
    table = client.Table('chentest')

    response = table.query(
        KeyConditionExpression=Key('id').eq(2) & Key('name').gt('C')
    )
    print(response)
    print("Printing items......")
    for item in response['Items']:
        print(item)


if __name__ == '__main__':
    get_item()
    query()
