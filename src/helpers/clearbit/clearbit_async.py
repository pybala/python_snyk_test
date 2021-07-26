import asyncio, aiohttp

AUTH_HEADER = ''
API_PERSON = 'https://person.clearbit.com/v2/combined/find?email={email_id}'


class ClearbitAsync:

    def __init__(self):
        self.session = None
        self.overQuota = False

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.session.close()

    async def getPersonByMail(self, emailId):
        if not self.overQuota:
            if self.session is None:
                self.session = aiohttp.ClientSession()

            apiUrl = API_PERSON.format(email_id=emailId)

            headers = {
                'Authorization': f'Bearer {AUTH_HEADER}',
                'Content-Type': 'application/json'
            }

            #quota limit error
            #{'error': {'type': 'over_quota', 'message': 'Your account is over its free quota'}}
            #https://clearbit.com/docs?python#errors
            try:
                async with self.session as session:
                    async with session.get(apiUrl, headers=headers, ssl=False, timeout=10) as response:
                        if 299 >= response.status >= 200:
                            cbData = await response.json()
                            return ClearbitAsync.getRequiredClearbitInfo(cbData)
                        elif response.status == 402:
                            self.overQuota = True
            except Exception as e:
                return {
                    'error': str(e)
                }

        return {
            'error': 'Error in process'
        }


    @staticmethod
    def getRequiredClearbitInfo(cbInfo):
        person = cbInfo['person']
        company = cbInfo['company']
        data = {
            'person': {},
            'company': {}
        }

        if person is not None:
            data['person'] = {
                'full_name': person['name']['fullName'],
                'title': person['employment']['title'],
                'location': person['location'],
                'city': person['geo']['city'],
                'state': person['geo']['state'],
                'state_code': person['geo']['stateCode'],
                'country': person['geo']['country'],
                'country_code': person['geo']['countryCode'],
                'avatar': person['avatar']
            }

            if person['linkedin']['handle'] != "":
                data['person']['linkedin'] = 'https://www.linkedin.com/' + person['linkedin']['handle']

        if company is not None:
            data['company'] = {
                'company_name': company['legalName'],
                'company_city': company['geo']['city'],
                'company_state': company['geo']['state'],
                'company_state_code': company['geo']['stateCode'],
                'company_country': company['geo']['country'],
                'company_country_code': company['geo']['countryCode'],
                'company_website': company['domain'],
                'sector': company['category']['sector'],
                'industry': company['category']['industry'],
                'company_logo': company['logo'],
                'company_linkedin': company['linkedin']['handle'],
                'ticker': company['ticker'],
                'employees': company['metrics']['employees'],
                'employees_range': company['metrics']['employeesRange']
            }

        return data


if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    email = 'test@test.com'

    cb = ClearbitAsync()

    result = loop.run_until_complete( cb.getPersonByMail(email) )
    print(result)

    loop.close()
