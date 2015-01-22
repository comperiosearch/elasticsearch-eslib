#!/usr/bin/env python
# -*- coding: utf-8 -*-

# TODO: LOGGING / OUTPUT OF ERRORS
# TODO: exception handling

import eslib  # For logging
from eslib.service import HttpService, status
import yaml, logging, argparse
import sys, os, imp, inspect, pwd, daemon, signal
from setproctitle import setproctitle

class ServiceRunner(object):
    def __init__(self, service, is_daemon):
        self._service = service
        self._reload = False
        self._terminate = False
        # Use service log here too
        self.log = service.log
        # Set up signal handlers
        signal.signal(signal.SIGABRT, self._sighandler_ABRT)  # abort and terminate service
        signal.signal(signal.SIGHUP , self._sighandler_HUP)   # reload service with new config
        signal.signal(signal.SIGINT , self._sighandler_INT)   # interrupt/stop processing (or map to SIGTERM)
        signal.signal(signal.SIGTERM, self._sighandler_TERM)  # terminate service and exit (normally)
        # When running as daemon, grab suspend/resume signals as well:
        if is_daemon:
            signal.signal(signal.SIGTSTP, self._sighandler_TSTP)  # suspend
            signal.signal(signal.SIGCONT, self._sighandler_CONT)  # resume

    def _sighandler_ABRT(self, sig, frame):
        self.log.info("Service wrapper received SIGABRT. Aborting any processing and terminating service.")
        self._service.processing_abort()
        self._service.processing_wait()
        self._service.shutdown()  # Not waiting
        self._reload = False
        self._terminate = True

    def _sighandler_HUP(self, sig, frame):
        info = None
        s = self._service.status
        self.log.info("Service wrapper received SIGHUP. Shutting down and reloading service.")
        self._service.shutdown()  # Not waiting
        self._reload = True

    def _sighandler_INT(self, sig, frame):
        self.log.info("Service wrapper received SIGINT. Shutting down and terminating.")
        self._service.shutdown()  # Not waiting
        self._terminate = True

    def _sighandler_TERM(self, sig, frame):
        self.log.info("Service wrapper received SIGTERM. Shutting down and terminating.")
        self._service.shutdown()  # Not waiting
        self._terminate = True

    def _sighandler_TSTP(self, sig, frame):
        s = self._service.status
        info = None
        doit = False
        if s == status.PROCESSING:
            info = "Suspending processing."
            doit = True
        elif s == status.SUSPENDED:
            info = "Processing is already suspended; no need to suspend."
        else:
            info = "Unable to suspend from state '%s'." % s
        self.log.info("Service wrapper received SIGTSTP. " + info)
        if doit:
            self._service.processing_resume()

    def _sighandler_CONT(self, sig, frame):
        s = self._service.status
        info = None
        doit = False
        if s == status.SUSPENDED:
            info = "Resuming processing."
            doit = True
        elif s == status.PROCESSING:
            info = "Already processing; no need to resume."
        else:
            info = "Unable to resume from state '%s'." % s
        self.log.info("Service wrapper received SIGCONT. " + info)
        if doit:
            self._service.processing_resume()

    def run(self):
        self._service.run(wait=True)
        return False if self._terminate else self._reload


def _list_services(mod, prefix="", ll=None):
    if not ll:
        ll = []
    for name, cls in inspect.getmembers(mod):
        if inspect.isclass(cls) and issubclass(cls, HttpService) and not issubclass(HttpService, cls):
            ll.append(prefix + name)
        elif inspect.ismodule(cls):
            _list_services(cls, "%s%s." % (prefix, name), ll)
    return ll

def list_services(run_dir=None, service_package_path=None, service_file_path=None):
    top = None
    if service_file_path:
        top = imp.load_source("service_module", service_file_path)
    elif service_package_path:
        top = imp.load_package("service_package", service_package_path)
    elif run_dir:
        top = imp.load_package("service_package", "/".join([run_dir, "service"]))
    else:
        print >> sys.stderr, "Either run directory, service_package or file path must be specified."
        sys.exit(1)

    print top

    services = eslib.unique(_list_services(top))
    for service in services:
        print service

def _find_type(mod, service_type_name):
    for name, cls in inspect.getmembers(mod):
        if inspect.isclass(cls) and issubclass(cls, HttpService) and not issubclass(HttpService, cls) and name == service_type_name:
            return cls
        elif inspect.ismodule(cls):
            hit = _find_type(cls, service_type_name)
            if hit:
                return hit
    return None

def run_service(
        run_dir=False, service_package_path=None, service_file_path=None,
        service_name=None, service_type_name=None, config_section=None,
        console_log_level=None,
        daemon_mode=None, run_as_user=None,
        manager_endpoint=None, endpoint=None):

    console_mode = not daemon_mode

    config_path      = "/".join([run_dir, "config", "services.yaml"])
    credentials_path = "/".join([run_dir, "config", "credentials.yaml"])
    log_config_path  = "/".join([run_dir, "config", "logging.yaml"])
    log_console_path = "/".join([run_dir, "config", "logging-console.yaml"])
    log_dir          = "/".join([run_dir, "log"   , service_name])

    # Change executing user (if privileged to do so)
    if run_as_user:
        pw = pwd.getpwnam(run_as_user)
        uid = pw.pw_uid
        gid = pw.pw_gid
        os.setgid(gid)
        os.setuid(uid)

    if not console_mode:
        # Change output dir to log_dir
        os.system('mkdir -p %s' % log_dir)
        os.chdir(log_dir)
        # Write pid
        with open("pid", "wt") as pidfile:
            print >> pidfile, os.getpid()

    reload = True
    while reload: # Only exceptions and 'reload' set to false will get us out of here

        # Find service type from name
        top = None
        if service_file_path:
            top = imp.load_source("service_module", service_file_path)
        elif service_package_path:
            top = imp.load_package("service_package", service_package_path)
        elif run_dir:
            top = imp.load_package("service_package", "/".join([run_dir, "service"]))
        if not top:
            print >> sys.stderr, "Either run directory, service_package or file path must be specified."
        service_type = _find_type(top, service_type_name)
        setproctitle("es-run-service %s" % service_name)

        # Load config files
        with open(credentials_path, "r") as f:
            credentials = yaml.load(f)
        with open(config_path, "r") as f:
            global_config = yaml.load(f)
        with open(log_console_path if console_mode else log_config_path) as f:
            log_config = yaml.load(f)

        # Initialize logging
        logging.config.dictConfig(config=log_config)

        if console_mode and console_log_level:
            for n in ["servicelog", "proclog", "doclog"]:
                logging.getLogger(n).setLevel(console_log_level or logging.INFO)

        # Instantiate and set up
        service = service_type(name=service_name)
        service.configure(credentials, global_config.get(config_section or service_name), global_config)
        if manager_endpoint:
            service.config.manager_endpoint = manager_endpoint
        if endpoint:
            service.config.management_endpoint = endpoint

        if daemon_mode:
            print "Daemonizing... redirecting stdout and stderr."
            # The daemon needs to preserve logging to files
            preserve = []
            loggers = list(logging.Logger.manager.loggerDict.values())
            loggers.append(logging.Logger.manager.root)
            for logger in loggers:
                if hasattr(logger, 'handlers'):
                    for handler in logger.handlers:
                        if hasattr(handler, 'stream') and \
                           hasattr(handler.stream, 'fileno'):
                            preserve.append(handler.stream)

            daemon_context = daemon.DaemonContext(
                stdout=open("daemon.stdout", "wb"),
                stderr=open("daemon.stderr", "wb"),
                files_preserve=preserve
            )
            with daemon_context:
                service.log.status("--- STARTING SERVICE ---")
                reload = ServiceRunner(service, daemon_mode).run()
        else:
            service.log.status("--- STARTING SERVICE ---")
            reload = ServiceRunner(service, daemon_mode).run()

        if reload:
            service.log.info("Reloading service.")

    if not console_mode:
        os.remove("pid")

    return reload

#================================

def main():
    usage = \
"""
Runner for eslib document processing services.

Usage:
  %(prog)s list [*options]
  %(prog)s [options] <service_name> <service_type>

Options:
  [-d <run_dir>]         * Home directory for service config and output.
  [-p <service_package>] * Override path to service package. Defaults to ./services.
  [-f <service_file>]    * Load types directly from module file instead of package.
  [-c <config_section>]    Name of config section for service within ./config/services.yaml
  [-l <log_level>]         Log level, for console mode logging only.
  [-u <run_as_user>]       Run as this user.
  [--daemon]               Detach and run as daemon process. (Otherwise logging to console, etc.)

  [-m <manager_endpoint>]  "host:port" of service manager, to subscribe to metadata.
  [-e <endpoint>]          "host:port" where this service listens for management requests.

Run dir expects directories with content in

    config/
        credentials.yaml
        logging.yaml
        logging-console.yaml
        services.yaml
    service/
        <python package with services>

and writes pid and log output to log/.
"""
    parser_desc = "Runner for eslib document processing services."
    parser = argparse.ArgumentParser(description=parser_desc)
    parser._actions[0].help = argparse.SUPPRESS

    if len(sys.argv) == 1:
        print usage.strip() % {"prog": parser.prog}
        sys.exit(1)

    if len(sys.argv) > 1 and sys.argv[1] == "list":
        sys.argv.remove("list")
        # Use a different parser instead
        desc = "List available service types."
        parser = argparse.ArgumentParser(description=desc)
        parser._actions[0].help = argparse.SUPPRESS
        parser.add_argument("-d", dest="run_dir"        , type=str, required=False)
        parser.add_argument("-p", dest="service_package", type=str, required=False)
        parser.add_argument("-f", dest="service_file"   , type=str, required=False)

        args = parser.parse_args()

        list_services(args.run_dir, args.service_package, args.service_file,)
    else:
        parser.add_argument("name", type=str, nargs=1)
        parser.add_argument("type", type=str, nargs=1)
        parser.add_argument("-d", dest="run_dir"        , type=str, required=False)
        parser.add_argument("-p", dest="service_package", type=str, required=False)
        parser.add_argument("-f", dest="service_file"   , type=str, required=False)
        parser.add_argument("-c", dest="config_section" , type=str, required=False)
        parser.add_argument("-l", dest="loglevel"       , type=str, required=False)
        parser.add_argument("-u", dest="user"           , type=str, required=False)
        parser.add_argument("--daemon" , action="store_true")
        parser.add_argument("-m", dest="mgr_endpoint"   , type=str, required=False)
        parser.add_argument("-e", dest="mgmt_endpoint"  , type=str, required=False)

        args = parser.parse_args()
        run_service(
            args.run_dir, args.service_package, args.service_file,
            args.name[0], args.type[0], args.config_section,
            args.loglevel,
            args.daemon, args.user,
            args.mgr_endpoint, args.mgmt_endpoint)

if __name__ == '__main__':
    main()
