__author__ = 'Hans Terje Bakke'


import urlparse, urllib


class UrlParamSpecification(object):
    def __init__(self):
        self.key        = None
        self.param_type = object
        self.required   = False
        self.multi_value = False

class UrlParamParser(object):
    """
    Parse URL into a dictionary of parameters, based on specification for how
    to parse the URLs path and query parameters.

    Path parameter notation, e.g.:

        {arg1}/{?optional_arg2}/text/{arg3}
        {arg1}/{arg2:float}/text/{*arg3}/text/{arg4}

    The ':float' here specifies that the type should be expected to be (or
    converted to) float during parsing.
    '?' means optional.
    '+' means required.
    '*' means multiple values are accepted and that the argument will be fit
    into a list.

    A path parameter cannot follow an optional parameter unless there is
    static text in the URL in between (for disambiguation.

    Definition notation for "query" parameters, e.g.:

        ["arg1:str", "*arg2:float"]

        arg1   : Create a variable with None value
        *arg2  : A list, multiple values allowed
        ?arg3  : Optional argument (default)
        +arg4  : Required argument
        ?*arg5 : Optional list/multi value

    If parameters are specified both for path and query params, they will
    be joined. If set to multi value in the query params spec, the result
    will be a list, even though only a singular entry is specified for path
    parsing.

    """

    def __init__(self, path_specification=None, param_specifications=None):
        """
        :param str         path_specifications  :
        :param list_of_str param_specifications :
        :raises ValueError: Missing key literal (in param specification)
        :raises ValueError: Failed to evaluate type (from path specification)
        """

        self._path = path_specification

        self._path_specifications = []   # List of str or UrlParamSpecification
        self._param_specifications = {}

        # URL query parameter specifications
        if param_specifications:
            for item in param_specifications:
                spec = self._parse_spec_string(item, default_required=False)
                if not spec.key:
                    raise ValueError("Missing key literal, key specification was '%s'." % item)
                self._param_specifications[spec.key] = spec  # Possibly replacing an existing spec

        # URL path parameter specifications
        if path_specification:
            previous_optional = False
            for part in path_specification.split("/"):
                if not part:
                    continue

                if len(part) >= 3 and part[0] == "{" and part[-1] == "}":
                    interior = part[1:-1]
                    spec = self._parse_spec_string(interior, default_required=True)

                    # Two optionals (incl. multi_value) in a row would be ambiguous and in not allowed
                    if not spec.required and previous_optional:
                        raise ValueError("Path specification for '%s' succeeds another optional/multi value spec; would be ambiguous and is not allowed." % spec.key)

                    # Check if it already exists in _param_specifications in a conflicting way
                    if spec.key in self._param_specifications:
                        other = self._param_specifications[spec.key]
                        if spec.param_type != other.param_type:
                            raise ValueError("Type mismatch in for same key ('%s') in path (type='%s') and param (type='%s') specs." % (spec.key, spec.param_type.__name__, other.param_type.__name__))
                        # If it is multi value in path, it must also be multi value in params
                        # The other direction is ok and handled during parsing, in which case it will parsed to a list
                        if spec.multi_value and not other.multi_value:
                            raise ValueError("Multi value in path spec and singular in param spec is not allowed, for key='%s'." % spec.key)

                    self._path_specifications.append(spec)
                    previous_optional |= (not spec.required)
                else:
                    self._path_specifications.append(part)
                    previous_optional = False

    def __str__(self):
        return "%s|%s" % (self.__class__.__name__, self.path)

    @property
    def path(self):
        return self._path

    def _parse_spec_string(self, spec_str, default_required=False):

        param_type = str  # default

        # Get type, if specified
        a = spec_str.split(":", 2)
        if len(a) == 2:
            key = a[0]
            exc = False
            try:
                param_type = eval(a[1])
            except Exception:
                exc = True
            if exc or not isinstance(param_type, type):
                raise ValueError("Failed to evaluate type from '%s' for specification '%s'." % (a[1], a[0]))
        else:
            key = spec_str

        required = default_required
        multi_value = False
        if key.startswith("?", 0, 1):
            required = False
            key = key[1:]
        elif key.startswith("+", 0, 1):
            required = True
            key = key[1:]
        if key.startswith("*", 0, 1):
            required = False
            multi_value = True
            key = key[1:]

        spec = UrlParamSpecification()
        spec.key         = key
        spec.param_type  = param_type
        spec.required    = required
        spec.multi_value = multi_value

        return spec

    def _str_to_type(self, key, param_type, param_values):
        converted = []
        for pv in param_values:
            cv = None
            if issubclass(param_type, basestring):
                cv = pv
            elif issubclass(param_type, bool):
                cv = False if pv in [None, "", "off", "no", "False", "false", "FALSE", "None", "none", "null", "NULL"] else True
            elif not pv is None:
                try:
                    cv = param_type(pv)
                except ValueError as e:
                    raise ValueError("Failed to convert str to %s for parameter '%s', value '%s': %s" % (param_type.__name__, key, pv, e))
            converted.append(cv)
        return converted


    def parse(self, url):
        """
        Parse parameters from URL path and query parameters according to specification.

        :param url:
        :return: A dict of parsed parameters.
        :raises AttributeError: Missing required attribute
        """

        final = {}
        parsed = urlparse.urlparse(url)
        path = urllib.unquote(parsed.path)

        # Add from path

        # If the path is incorrect, we return None, meaning "no match".
        # Because there could be other paths matching.

        parts = filter(None, path.split("/"))
        if not self._path_specifications and len(parts) > 0:
            return None  # No path match
        # First do a pass to build a list of potential matches and check whether the path format matches
        pairs = []
        part_i = 0
        for spec_i, spec in enumerate(self._path_specifications):

            if part_i >= len(parts):
                if type(spec) is UrlParamSpecification and not spec.required:
                    # We're at the end of the path with an optional spec being processed... it might be ok; go on
                    pairs.append((spec, [None]))
                    continue
                else:
                    # We've reached the end of the path before the spec was exhausted; so no path match
                    return None

            if not type(spec) is UrlParamSpecification:
                if spec == parts[part_i]:
                    part_i += 1
                else:
                    return None
            else:
                if spec.required:
                    pairs.append((spec, [parts[part_i]]))
                    part_i += 1
                else:
                    # We must now look ahead for something known
                    expected_str = next((item for item in self._path_specifications[spec_i:] if type(item) is not UrlParamSpecification), None)
                    if not expected_str:
                        known_i = len(parts)
                    else:
                        known_i = next((i for i in range(part_i, len(parts)) if parts[i] == expected_str), -1)
                    if known_i == -1:
                        return None  # We did not find the look-ahead token, so this is not a matching path

                    if known_i == part_i:
                        pairs.append((spec, [None]))
                        continue  # The optional parameter was missing, so we go on looking for the next spec at the same token position

                    if not spec.multi_value:
                        pairs.append((spec, [parts[part_i]]))
                        part_i += 1
                    else:
                        pairs.append((spec, parts[part_i:known_i]))
                        part_i = known_i
        if part_i < len(parts):
            return None  # There were more parts left of the path than our spec expected; so no path match

        # Now extract values from the matching parts
        for spec,pvalues in pairs:
            values = self._str_to_type(spec.key, spec.param_type, pvalues)
            final[spec.key] = filter(None, values) if spec.multi_value else values[-1]  # Get the last value in the list if singular

        # Add from params

        params = urlparse.parse_qs(parsed.query, keep_blank_values=True)
        for k,v in params.iteritems():
            if (len(v) == 1 and v[0] == ""):
                v[0] = None
        # Now 'params' is a dict of *list* values
        if self._param_specifications:
            for pkey, pvalues in params.iteritems():
                if pkey in self._param_specifications:
                    spec = self._param_specifications[pkey]
                    values = self._str_to_type(spec.key, spec.param_type, pvalues)
                    if spec.multi_value:
                        if pkey in final:
                            # It may exists in a non-list fashion from the path parsing; so we make sure to convert it to a list in that case
                            if type(final[pkey]) is not list:
                                final[pkey] = [final[pkey]]
                            final[pkey].extend(values)
                        else:
                            final[pkey] = values
                    else:
                        final[pkey] = values[-1]  # Get the last value in the list
                else:
                    pass  # Dropped

        # Finally, check if we have values for all required params
        for spec in self._param_specifications.itervalues():
            if not spec.key in final:
                if spec.required:
                    raise AttributeError("Missing required attribute '%s'." % spec.key)
                else:
                    final[spec.key] = None  # So the field exists, though not assigned a value

        return final
