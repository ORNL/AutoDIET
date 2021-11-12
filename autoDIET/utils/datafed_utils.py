import math
from datafed.CommandLib import API


def items_in_this_page(ls_resp, mode=None):
    """
    Returns a dictionary of data record and/or collections from the given
    listing response from DataFed

    Parameters
    ----------
    ls_resp : protobuf message
        Message containing the listing reply
    mode : str, optional
        Set to "d" to only get datasets from the listing
        Set to "c" to only get collections from the listing
        Leave unset for all items in the listing

    Returns
    -------
    dict
        Keys are IDs and values are the title for the object
    """
    data_objects = dict()

    if not mode:
        # return everything - records and collections
        for item in ls_resp[0].item:
            data_objects[item.title] = item.id
    else:
        for item in ls_resp[0].item:
            # return only those that match with the mode (dataset / collection)
            if item.id.startswith(mode):
                data_objects[item.title] = item.id

    return data_objects


def list_all_items_in_coll(coll_id_alias, context=None, mode=None,
                           df_api=None):
    """
    Returns a dictionary of data record and/or collections in the given
    collection

    Parameters
    ----------
    coll_id_alias : str
        ID or alias for DataFed collection
    context : str
        Context such as a project or user ID where this collection is located
        Not required if an ID is provided in place of an alias
    mode : str, optional
        Set to "d" to only get datasets from the listing
        Set to "c" to only get collections from the listing
        Leave unset for all items in the listing
    df_api : datafed.CommandLib.API instance
        Instance of the DataFed CommandLib API

    Returns
    -------
    dict
        Keys are IDs and values are the title for the object
    """
    if not df_api:
        df_api = API()
    # First do an LS
    ls_resp = df_api.collectionItemsList(coll_id_alias,
                                         context=context)
    # Gather important information like the number of pages, page size, etc.
    # TODO: perhaps request 100 items per request?
    page_size = ls_resp[0].count
    num_pages = math.ceil(ls_resp[0].total / ls_resp[0].count)
    # Get the items from the first page
    master_dict = items_in_this_page(ls_resp, mode=mode)
    # Get items from all subsequent pages
    for page_ind in range(1, num_pages):
        ls_resp = df_api.collectionItemsList(coll_id_alias,
                                             context=context,
                                             offset=page_size * page_ind)
        this_page = items_in_this_page(ls_resp, mode=mode)
        master_dict.update(this_page)

    return master_dict
