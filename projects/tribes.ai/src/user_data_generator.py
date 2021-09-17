"""
File that generates users data randomly that mimics data coming from user's mobiles
"""
from pathlib import Path
import argparse
from typing import List
from datetime import datetime, timedelta
import random
import json
from dateutil.parser import parse

import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "../"))
from project_config import app_config, USERS, LOGGER


class UserDataGenerator:
    """
    Class that generates users data
    """
    def __init__(self) -> None:
        pass

    @staticmethod
    def get_random_minutes(lower_limit: int, upper_limit: int, number_of_items: int) -> List[int]:
        """
        get random list of N integers between upper and lower limit
        :param lower_limit: lower limit
        :param upper_limit: upper limit
        :param number_of_items: random numbers N
        :return: int_list: list of randomly generated integers
        """
        int_list = []
        for j in range(number_of_items):
            int_list.append(random.randint(lower_limit, upper_limit - sum(int_list)))
        return int_list

    def generate_user_data(self, username: str, date: datetime.date) -> dict:
        """
        Function that generates following user data
        {
        user_id: "owais@tribes.ai",
        usages_date: "2021-09-15",
        "device": {"brand": "apple", "os": "ios"},
        "usages": [{"minute_used": 200 , "app_name": "slack", "app_category": "communication"}]
        }
        user_id, usages_date is an argument, device and usages are the random values from the list defined

        :param username: user for which data to generate
        :param date: date for which data to generate
        :return: random_user_data: randomly generated user data
        """
        random.shuffle(app_config['app_info'])
        usages = app_config['app_info'][:random.randint(1, len(app_config['app_info']))]
        minutes_used = self.get_random_minutes(app_config['lower_usage_limit'], app_config['upper_usage_limit'], len(usages))

        usages = [{**{"minute_used": minute_used}, **usage} for usage, minute_used in zip(usages, minutes_used)]
        random_user_data = {"user_id": f"{username}",
                            "usages_date": str(date),
                            "device": random.choice(app_config['device_info']),
                            "usages": usages
                            }
        return random_user_data

    def get_user_data(self, users: List[str] = None, date: str = None, save: bool = True, file_path:str = './src/data/user_data', **kwargs) -> List[dict]:
        """
        Save randomly generated data for the given users if 'save'==True else print out to console
        :param users: list of users whose data to generate
        :param date: date for which data to generate
        :param save: save data if True else print out to console
        :param file_path: path where to save files
        :return: list of user data if save=True else Null list
        """
        date = parse(date).date() if date else kwargs['execution_date'] if 'execution_date' in kwargs and kwargs['execution_date'] else datetime.now().date()

        LOGGER.info(f"Generating Data for users: {', '.join(users)} for date: {date}")
        users_data = []
        for user in users:
            user_data = self.generate_user_data(user, date=date)
            if save:
                Path(f"{file_path}/{user}@tribes.ai/").mkdir(parents=True, exist_ok=True)
                with open(f'{file_path}/{user}@tribes.ai/{str(date)}.json', 'w') as f:
                    json.dump(user_data, f)
            else:
                users_data.append(user_data)
        LOGGER.info(f"Successfully saved data to {file_path}") if save else LOGGER.info(f"Generated user data: {len(users_data)}")
        return users_data


if __name__ == "__main__":

    # argument parser: get list of users and bool for save
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--users', type=list, nargs='+', default=USERS,
                        help='user names')
    parser.add_argument('--save', type=bool, default=False, help='Save Data in file')
    args = vars(parser.parse_args())

    users = args['users']
    save = args['save']
    user_data_generator_obj = UserDataGenerator()

    # generate data for 30 days
    for i in range(30):
        users_data = user_data_generator_obj.get_user_data(users, save=save, date=str(datetime.now()+10*timedelta(days=i)))
