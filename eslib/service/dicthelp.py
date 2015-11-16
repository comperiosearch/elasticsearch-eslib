# -*- coding: utf-8 -*-

"""
eslib.service.dicthelp
~~~~~~~~~~

Module containing helpers for working with metadata (really just a dict).
"""


__all__ = ("get", "put", "remove_list_items", "delete")


from .. import unique


def _get_constrained_list_items(data_list, constraints, pass_non_dict=False):
    # Note: Comparing constraints to object values using unicode.
    ret = []
    for obj in data_list:
        if isinstance(obj, dict):
            ok = True
            for k,v in constraints.iteritems():
                if not (k in obj and unicode(obj[k]) == v):
                    ok = False
                    break  # Constraint test failed for object
            if ok:
                ret.append(obj)
        elif pass_non_dict:
            ret.append(obj)
    return ret

def _get_first_constrained_list_item(data_list, constraints, pass_non_dict=False):
    "Return first matching constrained item in the list, or None."
    # Note: Comparing constraints to object values using unicode.
    for obj in data_list:
        if isinstance(obj, dict):
            ok = True
            for k,v in constraints.iteritems():
                if not (k in obj and unicode(obj[k]) == v):
                    ok = False
                    break  # Constraint test failed for object
            if ok:
                return obj
        elif pass_non_dict:
            return obj
    return None

def _get_path_and_constraints(path):
    # Note: Converting constraints values to unicode.

    # Strip away the alias
    path = path.split("=>")[0].strip()

    cc = path.split("|")
    pp = cc[0].split(".")
    constraints = {}
    for c in cc[1:]:
        ss = c.split(":", 2)
        if len(ss) == 2:
            constraints[ss[0]] = unicode(ss[1])
    return pp, constraints

def _find_node(orig, pp, extend=False):

    # Find the way... extend: create new nodes if missing.
    node = orig
    for i, key in enumerate(pp[:-1]):
        if key in node:
            node = node[key]
            if not isinstance(node, dict):
                raise AttributeError("Node at path '%s' is not a dict." % ".".join(pp[:i+1]))
        elif extend:
            dd = {}
            node[key] = dd
            node = dd
        else:
            return None  # There is no such node
    return node

def get(orig, path):
    """
    Path format with opt constraints: dot.notation.path|key:value|key:value|...
    Non-intrusive.
    """
    pp, constraints = _get_path_and_constraints(path)
    node = _find_node(orig, pp)
    if node is None:
        return None  # Node not found

    key = pp[-1]
    target = None
    if not key:
        target = node
    elif not key in node:
        return None  # Key not found
    else:
        target = node[key]

    if isinstance(target, list):
        return _get_constrained_list_items(target, constraints, pass_non_dict=True)
    elif constraints:
        # Note: constraints only apply to lists
        return None
    else:
        return target

def pick_values(orig, path, subpath, flatten=False):
    items = []
    section = get(orig, path)
    if section:
        for part in section:
            subpart = get(part, subpath)
            if subpart:
                if flatten:  # Expect an array to be joined
                    items.extend(subpart)
                else:
                    items.append(subpart)
    return items

def put(orig, path, data, merge_lists=False):
    """
    Path format with opt constraints: dot.notation.path|key:value|key:value|...
    Overwrites existing section at path.
    """
    pp, constraints = _get_path_and_constraints(path)
    # Find the way... create new nodes if missing.
    node = _find_node(orig, pp, extend=True)

    # Now finally at the end. If it is a list already, we insert our (possibly constrained)
    # data into the list. If not, we simply overwrite.
    changed = True
    key = pp[-1]

    # Constrains mean we want to work with an array, and if there is no node, insert an array already
    if not key in node and constraints:
        node[key] = []

    if key in node and isinstance(node[key], list):
        existing_list = node[key]

        # Lists should replace lists or be merged
        if isinstance(data, list):
            if merge_lists:
                for d in data:
                    if not d in existing_list:
                        existing_list.append(d)
                        changed = True
            else:
                node[key] = data
                changed = True
        else:
            element = _get_first_constrained_list_item(existing_list, constraints)
            if element is None:
                if not data in existing_list:
                    existing_list.append(data)
                    changed = True
            else:
                if merge_lists:
                    if isinstance(data, list):
                        existing_list.extend(data)
                    else:
                        existing_list.append(data)
                else:
                    existing_list[existing_list.index(element)] = data
                changed = True
    else:
        node[key] = data
        changed = True

    return changed

def remove_list_items(orig, path, data_list):
    "Remove given item from list at path."

    if not isinstance(data_list, list):
        raise ValueError("Data must be a list.")

    pp = path.split(".")
    # Find the way... create new nodes if missing.
    node = _find_node(orig, pp, extend=True)

    # Now work on the target
    key = pp[-1]
    if not isinstance(node[key], list):
        return False  # Nothing was removed
    existing_list = node[key]
    removed = False
    for item in data_list:
        if item in existing_list:
            existing_list.remove(item)
            removed = True
    return removed

def _delete_at(constraints, collapse, node, pp, i):
    # returns (something_was_deleted, child_gone)
    if not isinstance(node, dict):
        if not (i == len(pp) - 1 ):
            raise AttributeError("Node at path '%s' is not a dict." % ".".join(pp[:i+1]))
        return (False, False)
    key = pp[i]
    if not pp[i] in node:
        return (False, False)
    if i == len(pp) - 1:  # Terminal
        if constraints:
            if isinstance(node[key], list):
                # Remove only list element matching constraint
                element = _get_first_constrained_list_item(node[key], constraints)
                if element:
                    node[key].remove(element)
                    return (True, node[key] == [])  # This means the entire child value (the list) is gone -- for collapsing.
            return (False, False)
        else:
            del node[key]
            return (True, True)  # Value/subtree now gone
    # Drill down
    deleted, child_gone = _delete_at(constraints, collapse, node[key], pp, i + 1)
    if collapse and child_gone and node[key] == {}:
        del node[key]
        return (True, True)  # Now this subtree is gone, too
    return (deleted, False)

def delete(orig, paths, collapse=False):
    """
    Removes sections in path with optional constraints 'dot.notation|key=val|key:val|...',
    optionally collapsing empty parent nodes.
    """
    for path in paths:
        pp, constraints = _get_path_and_constraints(path)
        deleted, child_gone = _delete_at(constraints, collapse, orig, pp, 0)
        return deleted

