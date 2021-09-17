import glob
import json
from datetime import datetime
from dateutil.parser import parse
from typing import List, Dict, Any, Optional
from schema import Schema, Or, SchemaError
from neomodel import StructuredNode, StringProperty, RelationshipTo, config, StructuredRel, DateTimeProperty, \
    FloatProperty, One
from neomodel import install_all_labels, db
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "../"))
from project_config import NEO4J_URL, LOGGER

config.DATABASE_URL = NEO4J_URL
install_all_labels()


# 'USED' Relationship model
class USED(StructuredRel):
    TimeCreated = DateTimeProperty(default=datetime.now())
    TimeEvent = DateTimeProperty()
    UsageMinutes = FloatProperty()


# 'ON' Relationship model
class ON(StructuredRel):
    TimeCreated = DateTimeProperty(default=datetime.now())


# 'OF' Relationship model
class OF(StructuredRel):
    TimeCreated = DateTimeProperty(default=datetime.now())


# User Node Model
class User(StructuredNode):
    IdMaster = StringProperty(unique_index=True)
    app_used = RelationshipTo('App', 'USED', model=USED)


# App Node Model
class App(StructuredNode):
    IdMaster = StringProperty(unique_index=True)
    AppCategory = StringProperty()
    device_on = RelationshipTo('Device', 'ON', model=ON)


# Device Node Model
class Device(StructuredNode):
    IdMaster = StringProperty(unique_index=True)
    brand_of = RelationshipTo('Brand', 'OF', model=OF)


# Brand Node Model
class Brand(StructuredNode):
    IdMaster = StringProperty(unique_index=True, cardinality=One)


class SaveToNeo4J:
    """
    class that saves data to Neo4j
    """

    def __init__(self, path: str = './src/data/user_data/**/') -> None:
        self.path = path

    @staticmethod
    def get_nodes_heartbeat() -> datetime:
        """
        Get last data stored in Neo4J
        :return: heartbeat
        """
        results, columns = db.cypher_query(
            """
            MATCH (:App)<-[u:USED]-(:User)
            With distinct u
            ORDER BY u.TimeEvent DESC
            Return collect(u.TimeEvent)[0] as heartbeat
            """,
        )
        heartbeat = datetime.fromtimestamp(results[0][0]) if results and results[0] and results[0][0] else None
        LOGGER.info(f"Neo4J Heartbeat: {heartbeat}")
        return heartbeat

    def read_user_data(self) -> List[Dict[str, Any]]:
        """
        read users data from json
        :return: list of latest users data to save
        """
        data = []
        files = glob.glob(self.path + '*.json')
        LOGGER.info(f"Received {len(files)} json files")
        heartbeat = self.get_nodes_heartbeat()
        filtered_files = list(
            filter(lambda file: parse(file[file.find('tribes.ai/') + 10:file.find('.json')]).date() > heartbeat.date(),
                   files)) if heartbeat else files
        LOGGER.info(f"Filtered Files: {len(filtered_files)}")
        for json_file in filtered_files:
            with open(json_file) as f:
                data.append(json.load(f))
        LOGGER.info(f"Fetch user data: {len(data)}")
        return data

    @staticmethod
    def create_node_if_not_exits(Node, **kwargs) -> StructuredNode:
        """
        Function that create a node in neo4j if not exists
        :param Node: Node Model Class
        :param kwargs:
        :return: Node class
        """
        if Node and not Node.nodes.first_or_none(IdMaster=kwargs['IdMaster']):
            LOGGER.info(f"Creating Node for {Node.__name__}: {kwargs['IdMaster']}")
            node = Node(**kwargs).save()
        else:
            node = Node.nodes.first_or_none(IdMaster=kwargs['IdMaster'])
        return node

    def send_data_to_neo4j(self, data: Optional[List[dict]] = None) -> None:
        """
        function that sends the data to neo4j
        :param data: data to send
        :return: None
        """
        try:
            data = data if data else self.read_user_data()
            schema = Schema([{'user_id': str, 'device': {"os": str, "brand": str}, "usages_date": Or(str, datetime),
                              "usages": [{"app_name": str, "app_category": str, "minute_used": Or(float, int)}]}])
            LOGGER.info("Validating fetched data...")
            validated_data = schema.validate(data)
            LOGGER.info("Validation successful!")
            del data
        except SchemaError as e:
            LOGGER.exception(f"Received Invalid User Data!\n{e}")
        else:
            for row in validated_data:
                user = self.create_node_if_not_exits(User, IdMaster=row['user_id'])
                device = self.create_node_if_not_exits(Device, IdMaster=row['device']['os'])
                brand = self.create_node_if_not_exits(Brand, IdMaster=row['device']['brand'])
                for usage in row['usages']:
                    app = self.create_node_if_not_exits(App, IdMaster=usage['app_name'], AppCategory=usage['app_category'])
                    user.app_used.connect(app, {"TimeEvent": parse(row['usages_date']), "UsageMinutes": usage['minute_used']}) if user and app else None
                    app.device_on.connect(device) if app and device and not app.device_on.is_connected(device) else None
                device.brand_of.connect(brand) if device and brand and not device.brand_of.is_connected(brand) else None

            LOGGER.info(f"Successfully created Nodes and Relationships")


if __name__ == "__main__":
    SaveToNeo4J().send_data_to_neo4j()
