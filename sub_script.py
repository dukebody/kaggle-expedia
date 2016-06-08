# coding: utf-8
from datetime import datetime
from heapq import nlargest
from operator import itemgetter
import math


def get_season(month):
    if month in (11, 12, 1, 2):
        return 'winter'
    elif month in (3, 4, 5, 6):
        return 'spring'
    elif month in (7, 8):
        return 'summer'
    return 'fall'


def parse_date(date_str):
    year_str = date_str[:4]
    month_str = date_str[5:7]
    day_str = date_str[8:10]
    return datetime(int(year_str), int(month_str), int(day_str))


def not_blank(*strings):
    """
    Return True if all given inputs are not blank strings.
    """
    return all([val != '' for val in strings])


def append_rule(antecedents, consequent, weight, rule_collection):
    """
    Antecedents are usually a tuple of matched attributes and consequent
    is a hotel cluster.
    weight is the weight assigned to the generated rule, instead of just 1.
    rule_collection is the collection to append the generated rule to.
    """
    if antecedents in rule_collection:
        if consequent in rule_collection[antecedents]:
            rule_collection[antecedents][consequent] += weight
        else:
            rule_collection[antecedents][consequent] = weight
        rule_collection[antecedents]['__all__'] += weight
    else:
        # rule_collection[antecedents] = dict()
        rule_collection[antecedents] = {'__all__': weight}
        rule_collection[antecedents][consequent] = weight


def apply_rules(antecedents, consequents, rule_collection, out):
    """
    Apply the given rule collection to the antecedents, writing the
    consequents to out.
    Return number of added consequents.
    """
    new_consequents = 0
    if antecedents in rule_collection:
        rule = rule_collection[antecedents]
        # print(rule)
        support_antecedents = float(rule['__all__']) + 1e-9
        # print(support_antecedents)
        rule_items = [(cluster, support / support_antecedents) for cluster, support in rule.items() if cluster != '__all__']
        topitems = nlargest(5, rule_items, key=itemgetter(1))
        # if cluster already in consequents, add only if higher confidence
        for topitem in topitems:
            cluster_id, new_confidence = topitem
            if cluster_id in consequents.keys():
                if consequents[cluster_id] < new_confidence:
                    consequents[cluster_id] = new_confidence
                    new_consequents += 1
            else:  # otherwise just add it to the list
                consequents[cluster_id] = new_confidence
                new_consequents += 1
        # for i in range(len(topitems)):
        #     if topitems[i][0] in consequents:
        #         continue
        #     if len(consequents) == 5:
        #         break
        #     out.write(' ' + topitems[i][0])
        #     consequents.append(topitems[i][0])
        #     new_consequents += 1
    return new_consequents


def prepare_arrays_match():
    f = open("t1.csv", "r")
    f.readline()

    rule_names = [
        'best_hotels_od_ulc', 'best_hotels_uid_miss',
        'best_hotels_search_dest', 'best_hotels_country',
        'popular_hotel_cluster', 'best_s00', 'best_s01', 'best_hotels_dates',
        'best_hotels_season'
    ]
    rule_collections = {rule_name: dict() for rule_name in rule_names}
    popular_hotel_cluster = dict()
    total = 0

    # Calc counts
    while 1:
        line = f.readline().strip()
        total += 1

        if total % 2000000 == 0:
            print('Read {} lines...'.format(total))

        if line == '':
            break

        arr = line.split(",")

        if arr[11] != '':
            book_year = int(arr[11][:4])
            book_month = int(arr[11][5:7])
        else:
            book_year = int(arr[0][:4])
            book_month = int(arr[0][5:7])

        if (book_month < 1 or book_month > 12 or book_year < 2012 or
                book_year > 2015):
            # print(book_month)
            # print(book_year)
            # print(line)
            continue

        # data leak
        date_time = arr[0]
        user_location_country = arr[3]
        user_location_region = arr[4]

        user_location_city = arr[5]
        orig_destination_distance = arr[6]
        user_id = arr[7]
        is_package = arr[9]
        booking_ini = arr[11]
        booking_end = arr[12]
        srch_destination_id = arr[16]
        hotel_country = arr[21]
        hotel_market = arr[22]
        is_booking = float(arr[18])
        hotel_cluster = arr[23]

        append_0 = ((book_year - 2012) * 12 + (book_month - 12))
        if not (append_0 > 0 and append_0 <= 36):
            # print(book_year)
            # print(book_month)
            print(line)
            # print(append_0)
            continue

        append_1 = pow(math.log(append_0), 1.3) * \
            pow(append_0, 1.46) * (3.5 + 17.6 * is_booking)
        append_2 = 3 + 5.56 * is_booking

        # Miquel
        # generate vars
        created_dt = parse_date(date_time)
        weekday_created = created_dt.weekday()

        if not_blank(booking_ini):
            booking_ini_dt = parse_date(booking_ini)
            month_booking = booking_ini_dt.month
            season_booking = get_season(month_booking)
            weekday_booking = booking_ini_dt.weekday()
        else:
            weekday_booking = -1
        if not_blank(booking_ini, booking_end):
            booking_end_dt = parse_date(booking_end)
            stay_duration = (booking_end_dt - booking_ini_dt).days
        else:
            stay_duration = -1

        # weekday + duration
        if not_blank(srch_destination_id, hotel_country) and is_booking:
            w = (weekday_booking, stay_duration,
                 srch_destination_id, hotel_country, hotel_market)
            best_hotels_dates = rule_collections['best_hotels_dates']
            append_rule(w, hotel_cluster, append_0, best_hotels_dates)

        # month
        if False:  # srch_destination_id != '' and hotel_country != '' and is_booking == 1:
            w = (season_booking, srch_destination_id)
            best_hotels_season = rule_collections['best_hotels_season']
            append_rule(w, hotel_cluster, append_1, best_hotels_season)
        # End of Miquel

        if (not_blank(user_location_city, orig_destination_distance,
                      user_id, srch_destination_id, hotel_country) and
                is_booking):
            s00 = (user_id, user_location_city,
                   srch_destination_id, hotel_country, hotel_market)
            best_s00 = rule_collections['best_s00']
            append_rule(s00, hotel_cluster, append_0, best_s00)

        if (not_blank(user_location_city, orig_destination_distance, user_id,
                      srch_destination_id) and is_booking):
            s01 = (user_id, srch_destination_id, hotel_country, hotel_market)
            best_s01 = rule_collections['best_s01']
            append_rule(s01, hotel_cluster, append_0, best_s01)

        if (not_blank(user_location_city, user_id,
                      srch_destination_id, hotel_country) and
                orig_destination_distance == '' and is_booking):
            s0 = (user_id, user_location_city,
                  srch_destination_id, hotel_country, hotel_market)
            best_hotels_uid_miss = rule_collections['best_hotels_uid_miss']
            append_rule(s0, hotel_cluster, append_0, best_hotels_uid_miss)

        if not_blank(user_location_city, orig_destination_distance):
            s1 = (user_location_city, orig_destination_distance)
            best_hotels_od_ulc = rule_collections['best_hotels_od_ulc']
            append_rule(s1, hotel_cluster, append_0, best_hotels_od_ulc)

        if not_blank(srch_destination_id, hotel_country, hotel_market):
            s2 = (srch_destination_id, hotel_country, hotel_market, is_package)
            best_hotels_search_dest = \
                rule_collections['best_hotels_search_dest']
            append_rule(s2, hotel_cluster, append_1, best_hotels_search_dest)

        if not_blank(hotel_market):
            s3 = (hotel_market)
            best_hotels_country = rule_collections['best_hotels_country']
            append_rule(s3, hotel_cluster, append_2, best_hotels_country)

        if hotel_cluster in popular_hotel_cluster:
            popular_hotel_cluster[hotel_cluster] += append_0
        else:
            popular_hotel_cluster[hotel_cluster] = append_0

    f.close()
    return rule_collections, popular_hotel_cluster


def gen_submission(rule_collections, popular_hotel_cluster):
    # now = datetime.datetime.now()
    # path = 'submission_' + str(now.strftime("%Y-%m-%d-%H-%M")) + '.csv'
    # path = 'submission_last.csv'
    path = 'submission_last.csv'
    out = open(path, "w")
    f = open("t2.csv", "r")
    f.readline()
    total = 0
    total0 = 0
    total00 = 0
    total1 = 0
    total2 = 0
    total3 = 0
    total4 = 0
    totalw = 0
    totalseason = 0
    out.write("id,hotel_cluster\n")
    topclasters = nlargest(
        5, sorted(popular_hotel_cluster.items()), key=itemgetter(1))

    while 1:
        line = f.readline().strip()
        total += 1

        if total % 100000 == 0:
            print('Write {} lines...'.format(total))

        if line == '':
            break

        arr = line.split(",")
        # print(arr)
        id = arr[0]

        date_time = arr[1]
        # data leak
        user_location_country = arr[4]
        user_location_region = arr[5]

        user_location_city = arr[6]
        orig_destination_distance = arr[7]
        user_id = arr[8]
        is_package = arr[10]
        booking_ini = arr[12]
        booking_end = arr[13]
        srch_destination_id = arr[17]
        hotel_country = arr[20]
        hotel_market = arr[21]

        out.write(str(id) + ',')
        filled = {}

        # feat eng
        try:
            weekday_created = parse_date(date_time).weekday()
            if booking_ini != '':
                booking_ini_dt = parse_date(booking_ini)
                month_booking = booking_ini_dt.month
                season_booking = get_season(month_booking)
                weekday_booking = booking_ini_dt.weekday()

            else:
                weekday_booking = -1
            if booking_ini != '' and booking_end != '':
                booking_end_dt = parse_date(booking_end)
                stay_duration = (booking_end_dt - booking_ini_dt).days
            else:
                stay_duration = -1
        except Exception:
            weekday_created = -1
            weekday_booking = -1
            stay_duration = -1
        ###

        # data leak
        s1 = (user_location_city, orig_destination_distance)
        total1 += apply_rules(s1, filled,
                              rule_collections['best_hotels_od_ulc'], out)

        if orig_destination_distance == '':
            s0 = (user_id, user_location_city,
                  srch_destination_id, hotel_country, hotel_market)
            total0 += apply_rules(
                    s0, filled, rule_collections['best_hotels_uid_miss'], out)

        s00 = (user_id, user_location_city,
               srch_destination_id, hotel_country, hotel_market)
        s01 = (user_id, srch_destination_id, hotel_country, hotel_market)
        if s01 in rule_collections['best_s01'] and \
                s00 not in rule_collections['best_s00']:
            total00 += apply_rules(s01, filled, rule_collections['best_s01'],
                                   out)

        s2 = (srch_destination_id, hotel_country, hotel_market, is_package)
        total2 += apply_rules(
                s2, filled, rule_collections['best_hotels_search_dest'], out)

        # weekend
        sw = (weekday_booking, stay_duration,
              srch_destination_id, hotel_country, hotel_market)
        totalw += apply_rules(
            sw, filled, rule_collections['best_hotels_dates'], out)

        # season
        ss = (season_booking, srch_destination_id)
        if False:  # not_blank(srch_destination_id, hotel_country):
            totalseason += apply_rules(
                          ss, filled, rule_collections['best_hotels_season'],
                          out)

        s3 = (hotel_market)
        total3 += apply_rules(
            s3, filled, rule_collections['best_hotels_country'], out)

        clusters_conf = filled.items()
        # print(clusters_conf)
        clusters_conf = sorted(clusters_conf, key=lambda x: -x[1])[:5]
        for cluster_conf in clusters_conf:
            out.write(' ' + cluster_conf[0])

        if len(clusters_conf) < 5:
            my_topclasters = topclasters[:5 - len(clusters_conf)]
            total4 += len(my_topclasters)
            for topclaster in my_topclasters:
                out.write(' ' + topclaster[0])
        out.write("\n")
    out.close()
    print('Total 1: {} ...'.format(total1))
    print('Total 0: {} ...'.format(total0))
    print('Total w: {} ...'.format(totalw))
    print('Total season: {} ...'.format(totalseason))
    print('Total 00: {} ...'.format(total00))
    print('Total 2: {} ...'.format(total2))
    print('Total 3: {} ...'.format(total3))
    print('Total 4: {} ...'.format(total4))


rule_collections, popular_hotel_cluster = prepare_arrays_match()
gen_submission(rule_collections, popular_hotel_cluster)
