#!/usr/bin/env python
# -*- coding: utf-8 -*-

# The purpose of this service is to launch and kill local services as requested by the service manager.
# Note: This service needs not enter "processing" mode, as there is no document processing or timer.

from eslib.service import HttpService, PipelineService
from eslib.procs import Timer
from eslib.esdoc import tojson
import os, signal, subprocess, sys, yaml

class ServiceLauncherBase(HttpService, PipelineService):

    def __init__(self, **kwargs):
        super(ServiceLauncherBase, self).__init__(**kwargs)

        self.config.set_default(
            management_endpoint     = "localhost:5000", # for this service...
            service_runner          = None,
            service_dir             = None,
        )

    def on_configure(self, credentials, config, global_config):
        self.config.set(
            management_endpoint     = config.get("management_endpoint") or self.config.management_endpoint,
            service_runner          = config.get("service_runner"),
            service_dir             = config.get("service_dir"),
        )


    def get_run_dir(self):
        return self.config.service_dir or os.path.normpath(os.path.join(os.getcwd(), "../.."))

    def load_config(self):
        # Load config
        run_dir = self.get_run_dir()
        config_path = self.config_file
        if not config_path:
            config_path  = "/".join([run_dir, "config", "services.yaml"])
        credentials_path = "/".join([run_dir, "config", "credentials.yaml"])
        with open(credentials_path, "r") as f:
            credentials = yaml.load(f)
        with open(config_path, "r") as f:
            global_config = yaml.load(f)
        config_dict = {"credentials": credentials, "config": global_config}
        return config_dict

    def spawn(self, id_, config_dict, config_key, addr, manager_endpoint, start):
        runner = self.config.service_runner or sys.argv[0]
        run_dir = self.get_run_dir()

        # Prepare arguments
        args = [
            sys.executable,  # Same python that is running this
            runner,
            "-d", run_dir,
            id_,
            "-m", manager_endpoint,
            "-e", addr,
            "--stdincfg",  # Always send credentials and config via stdin here
            "--daemon"     # Needed for logging to directories anyway..
        ]
        if start:
            args.append("--start")
        if self.config_file:
            args.extend(["-f", self.config_file]) # Config file is just for the record here.. only launchers use that.
        if config_key:
            args.extend(["-c", config_key])

        # Start process
        p = None
        p = subprocess.Popen(
            args,
            stdout = subprocess.PIPE,
            stderr = subprocess.PIPE,
            stdin = subprocess.PIPE
        )
        # TODO: Grab stdout/stderr
        if config_dict is not None:
            p.stdin.write(tojson(config_dict))
        p.stdin.close()
        return p


class ServiceLauncher(ServiceLauncherBase):

    def __init__(self, **kwargs):
        super(ServiceLauncher, self).__init__(**kwargs)

        self.config.set_default(
            keepalive_frequency     = 60.0  # 10 seconds
        )

        self.add_route(self._mgmt_service_launch    , "POST|PUT", "/launch"     , None)
        self.add_route(self._mgmt_service_kill      , "DELETE"  , "/kill"       , None)

    def on_configure(self, credentials, config, global_config):
        super(ServiceLauncher, self).on_configure(credentials, config, global_config)
        self.config.set(
            manager_endpoint        = config.get("manager_endpoint") or self.config.manager_endpoint,
            keepalive_frequency     = config.get("keepalive_frequency") or self.config.keepalive_frequency
        )

    def on_setup(self):
        self._timer = Timer(actions=[(self.config.keepalive_frequency, self.config.keepalive_frequency, "ping")])
        self._timer.add_callback(self._say_hello)

        procs = [self._timer]

        # Link procs (just one in this case..)
        self.link(*procs)

        #  Register them for debug dumping
        self.register_procs(*procs)

        return True

    def _say_hello(self, proc, doc):
        # The manager may have booted since this service registered, and in case we only registered as guest, we
        # should attempt to re-register just to make sure the manager knows about us. If we are permanently registered,
        # (i.e. not "guest", then this is unnecessary.

        data = self._build_hello_message(self.config.management_endpoint)
        if self.config.manager_endpoint and not self.config.manager_endpoint == "standalone":
            # Say hello to manager
            self.log.debug("Sending keepalive 'hello' message to manager.")
            try:
                content = self.remote(self.config.manager_endpoint, "post", "hello", data=data)
                error = content.get("error")
                if error:
                    self.log.error("Error from manager: %s" % error)
                    return
            except Exception as e:
                self.log.warning("Communication with manager failed for 'hello' message: %s" % e)
                return
            # Apply metadata from response
            metablock = content.get("metadata")
            if metablock:
                try:
                    self.update_metadata(metablock.get("version"), metablock.get("data"), wait=True)
                except Exception as e:
                    self.log.exception("Error parsing metadata. But proceeding...")

    #region Service interface commands

    def _mgmt_service_launch(self, request_handler, payload, **kwargs):
        id = payload.get("id") or []
        self.log.debug("called: launch service '%s'" % id)
        return self._launch_service(
            id,
            payload.get("config"),
            payload.get("config_key"),
            payload["endpoint"],
            payload["manager_endpoint"],
            payload.get("start") or False
        )

    def _mgmt_service_kill(self, request_handler, payload, **kwargs):
        id = payload.get("id")
        pid = int(payload.get("pid"))
        self.log.debug("called: kill service '%s', pid '%s'" % (id, pid))
        return self._kill_services(
            id,
            pid,
            payload.get("force") or False
        )

    #endregion Service interface commands

    #region Service interface helpers

    def _launch_service(self, id, config_dict, config_key, addr, manager_endpoint, start):

        self.log.debug("Launching service '%s' at '%s'." % (id, addr))

        p = None
        try:
            p = self.spawn(id, config_dict, config_key, addr, manager_endpoint, start)
        except Exception as e:
            self.log.exception("Failed to launch service '%s' at '%s'." % (id, addr))
            return {
                "pid": 0,
                "error": "Failed to launch service '%s' at '%s': %s: %s" % (id, addr, e.__class__.__name__, e)
            }

        msg = "Service '%s' launched at '%s' with pid=%d." % (id, addr, p.pid)
        self.log.status(msg)

        return {"pid": p.pid, "message": msg}

    def _kill_services(self, id, pid, force):
        # This is only for extreme cases.
        # Normally, a shutdown should do the trick.

        dead = False

        try:
            if force:
                self.log.debug("Forcefully kill service '%s', pid=%d." % (id, pid))
            else:
                self.log.debug("Attempting to kill service '%s', pid=%d." % (id, pid))
            os.kill(pid, signal.SIGKILL if force else signal.SIGTERM)
        except OSError as e:
            self.log.debug("Killing service '%s', pid=%d, failed. errno=%d" % (id, pid, e.errno))
            if e.errno == 3:  # does not exist
                dead = True  # Although kill failed, consider it dead (because it does no longer exist)
        else:
            # SIGTERM kills should cause a shutdown which will later cause a 'goodbye' notification back to this manager.
            if force:
                dead = True  # Consider it gone

        return {"killed": dead}

    #endregion Service interface helpers
