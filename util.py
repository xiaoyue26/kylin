def uid_list_to_set(input_list):
    return_list = []
    for i in input_list:
        tmp_set = set()
        for j in i:
            tmp_set.add(j[0])
        return_list.append(tmp_set)
    return return_list


def uid_arithmetic(user_id, arithmetic_method):
    uid_list = user_id
    res = eval(arithmetic_method)
    return res
