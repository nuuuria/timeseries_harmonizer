
class Processor(object):
    name = None

    @staticmethod
    def get_devices(frequency):
        raise NotImplementedError

    @staticmethod
    def process_device(pv_system, druid_producer, druid_connection, druid_datasource, druid_topic, influx_connection,
                       ts_ini, ts_end, freq):
        raise NotImplementedError

