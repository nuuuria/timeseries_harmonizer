import argparse
import datetime
import os
import time
import pandas as pd
import yaml
from dateutil.relativedelta import relativedelta
from kubernetes import config, client, utils
from jinja2 import Environment, FileSystemLoader


def get_job_status(batch, job_name, namespace):
    try:
        job = batch.read_namespaced_job(job_name, namespace)
    except:
        return None
    return job


KUBECONFIG = "~/.kube/config"

freq_list = [("PT15M", 10, relativedelta(months=1, days=10), "1m"),
             ("PT1H", 10, relativedelta(months=1, days=10), "1m"),
             ("P1D", 4, relativedelta(months=6), "5m"),
             ("P1M", 4, relativedelta(months=8), "5m"),
             ("P1W", 4, relativedelta(months=6), "5m")]


def launch_automatically(start, stop, frequencies, actions):
    config.load_kube_config(KUBECONFIG)
    env = Environment(
        loader=FileSystemLoader('kubejob'),
        variable_start_string='${',
        variable_end_string='}'
    )
    template_starter = env.get_template("harmonizer-job-starter.yaml")
    template_consumer = env.get_template("harmonizer-job-consumer.yaml")
    for freq, proc, delta, step in frequencies:
        for s in pd.date_range(start,
                               stop,
                               freq=step):
            # set parameters for launching the jobs
            actions_ = ", ".join([f"\"{a}\"" for a in actions])
            params = {"TOPIC": '"-t", "icat.influx"',
                      "SOURCEX": f'"-d", "influx-hist",',
                      "SOURCE": "influx-hist",
                      "PROCX": f'"-n", "{proc}",',
                      "PROC": proc,
                      "ACTIONS": f'"-a", {actions_},',
                      "FREQ": f',"-f", "{freq}",',
                      "START": f'"-s", "{s.strftime("%Y-%m-%dT%H:%M:%S+00:00")}",',
                      "STOP": f'"-p", "{(s + delta).strftime("%Y-%m-%dT%H:%M:%S+00:00")}",'
                      }
            print(params)
            yaml_string_s = template_starter.render(params)
            yaml_string_c = template_consumer.render(params)
            yaml_obj_s = yaml.safe_load_all(yaml_string_s)
            yaml_obj_c = yaml.safe_load_all(yaml_string_c)
            k8s_client = client.ApiClient()
            utils.create_from_yaml(k8s_client, yaml_objects=yaml_obj_s)
            time.sleep(60)
            utils.create_from_yaml(k8s_client, yaml_objects=yaml_obj_c)
            batch = client.BatchV1Api()
            j = get_job_status(batch, "harmonizerv2-job-consumer-influx-hist", "harmonizer-icat")
            while j and j.status.active:
                time.sleep(1)
                j = get_job_status(batch, "harmonizerv2-job-consumer-influx-hist", "harmonizer-icat")
            try:
                delete_options = client.V1DeleteOptions(propagation_policy="Foreground")
                batch.delete_namespaced_job("harmonizerv2-job-consumer-influx-hist", "harmonizer-icat",
                                            body=delete_options)
                batch.delete_namespaced_job("harmonizerv2-job-starter-influx-hist", "harmonizer-icat",
                                            body=delete_options)
            except:
                pass
            j = get_job_status(batch, "harmonizerv2-job-consumer-influx-hist", "harmonizer-icat")
            while j:
                time.sleep(1)
                j = get_job_status(batch, "harmonizerv2-job-consumer-influx-hist", "harmonizer-icat")
            j = get_job_status(batch, "harmonizerv2-job-starter-influx-hist", "harmonizer-icat")
            while j:
                time.sleep(1)
                j = get_job_status(batch, "harmonizerv2-job-starter-influx-hist", "harmonizer-icat")
            time.sleep(30)


def frequency(value):
    for x in freq_list:
        if value == x[0]:
            return x
    raise ValueError(f"{value} is not a valid frequency")


def date(value):
    return datetime.datetime.fromisoformat(value)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument('--freq', '-f', required=False, default=freq_list,
                    nargs='*', type=frequency)
    ap.add_argument('--start', '-s', required=False,
                    default=datetime.datetime.fromisoformat("2024-01-01T00:00:00+00:00"), type=date)
    ap.add_argument('--stop', '-p', required=False,
                    default=datetime.datetime.now(tz=datetime.timezone.utc), type=date)
    ap.add_argument("--actions", "-a", required=False, nargs="+", default=["raw", "processors", "calculations", "limits"])

    if os.getenv("PYCHARM_HOSTED") is not None:
        print("PYCHARM_HOSTED")
        args = ap.parse_args([])
    else:
        args = ap.parse_args()
        launch_automatically(args.start, args.stop, args.freq, args.actions)
