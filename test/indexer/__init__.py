from azul import config
from azul.project.hca.config import IndexProperties
from azul.project.hca.indexer import Indexer
from shared import AzulTestCase
import os


class IndexerTestCase(AzulTestCase):
    index_properties = None
    hca_indexer = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.old_dss_endpoint = os.environ.get('AZUL_DSS_ENDPOINT')
        os.environ['AZUL_DSS_ENDPOINT'] = "https://dss.data.humancellatlas.org/v1"
        cls.index_properties = IndexProperties(dss_url=config.dss_endpoint,
                                               es_endpoint=(cls.es_host_ip,
                                                            cls.es_host_port))
        cls.hca_indexer = Indexer(cls.index_properties)
