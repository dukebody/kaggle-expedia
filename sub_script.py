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
    return all([val != '' for val in strings])


def prepare_arrays_match():
    f = open("t1.csv", "r")
    f.readline()

    best_hotels_od_ulc = dict()
    best_hotels_uid_miss = dict()
    best_hotels_search_dest = dict()
    best_hotels_country = dict()
    popular_hotel_cluster = dict()
    best_s00 = dict()
    best_s01 = dict()
    best_hotels_dates = dict()
    best_hotels_season = dict()
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
            if w in best_hotels_dates:
                if hotel_cluster in best_hotels_dates[w]:
                    best_hotels_dates[w][hotel_cluster] += append_0
                else:
                    best_hotels_dates[w][hotel_cluster] = append_0
            else:
                best_hotels_dates[w] = dict()
                best_hotels_dates[w][hotel_cluster] = append_0

        # month
        if False:  # srch_destination_id != '' and hotel_country != '' and is_booking == 1:
            w = (season_booking, srch_destination_id)
            if w in best_hotels_season:
                if hotel_cluster in best_hotels_season[w]:
                    best_hotels_season[w][hotel_cluster] += append_1
                else:
                    best_hotels_season[w][hotel_cluster] = append_1
            else:
                best_hotels_season[w] = dict()
                best_hotels_season[w][hotel_cluster] = append_1
        # End of Miquel

        if (not_blank(user_location_city, orig_destination_distance,
                      user_id, srch_destination_id, hotel_country) and
                is_booking):
            s00 = (user_id, user_location_city,
                   srch_destination_id, hotel_country, hotel_market)
            if s00 in best_s00:
                if hotel_cluster in best_s00[s00]:
                    best_s00[s00][hotel_cluster] += append_0
                else:
                    best_s00[s00][hotel_cluster] = append_0
            else:
                best_s00[s00] = dict()
                best_s00[s00][hotel_cluster] = append_0

        if (not_blank(user_location_city, orig_destination_distance, user_id,
                      srch_destination_id) and is_booking):
            s01 = (user_id, srch_destination_id, hotel_country, hotel_market)
            # print(s01)
            if s01 in best_s01:
                if hotel_cluster in best_s01[s01]:
                    best_s01[s01][hotel_cluster] += append_0
                else:
                    best_s01[s01][hotel_cluster] = append_0
            else:
                best_s01[s01] = dict()
                best_s01[s01][hotel_cluster] = append_0

        if (not_blank(user_location_city, user_id,
                      srch_destination_id, hotel_country) and
                orig_destination_distance == '' and is_booking):
            s0 = (user_id, user_location_city,
                  srch_destination_id, hotel_country, hotel_market)
            if s0 in best_hotels_uid_miss:
                if hotel_cluster in best_hotels_uid_miss[s0]:
                    best_hotels_uid_miss[s0][hotel_cluster] += append_0
                else:
                    best_hotels_uid_miss[s0][hotel_cluster] = append_0
            else:
                best_hotels_uid_miss[s0] = dict()
                best_hotels_uid_miss[s0][hotel_cluster] = append_0

        if not_blank(user_location_city, orig_destination_distance):
            s1 = (user_location_city, orig_destination_distance)

            if s1 in best_hotels_od_ulc:
                if hotel_cluster in best_hotels_od_ulc[s1]:
                    best_hotels_od_ulc[s1][hotel_cluster] += append_0
                else:
                    best_hotels_od_ulc[s1][hotel_cluster] = append_0
            else:
                best_hotels_od_ulc[s1] = dict()
                best_hotels_od_ulc[s1][hotel_cluster] = append_0

        if not_blank(srch_destination_id, hotel_country, hotel_market):
            s2 = (srch_destination_id, hotel_country, hotel_market, is_package)
            if s2 in best_hotels_search_dest:
                if hotel_cluster in best_hotels_search_dest[s2]:
                    best_hotels_search_dest[s2][hotel_cluster] += append_1
                else:
                    best_hotels_search_dest[s2][hotel_cluster] = append_1
            else:
                best_hotels_search_dest[s2] = dict()
                best_hotels_search_dest[s2][hotel_cluster] = append_1

        if not_blank(hotel_market):
            s3 = (hotel_market)
            if s3 in best_hotels_country:
                if hotel_cluster in best_hotels_country[s3]:
                    best_hotels_country[s3][hotel_cluster] += append_2
                else:
                    best_hotels_country[s3][hotel_cluster] = append_2
            else:
                best_hotels_country[s3] = dict()
                best_hotels_country[s3][hotel_cluster] = append_2

        if hotel_cluster in popular_hotel_cluster:
            popular_hotel_cluster[hotel_cluster] += append_0
        else:
            popular_hotel_cluster[hotel_cluster] = append_0

    f.close()
    return best_s00, best_s01, best_hotels_country, best_hotels_od_ulc, best_hotels_uid_miss, best_hotels_search_dest, popular_hotel_cluster, best_hotels_dates, best_hotels_season


def gen_submission(best_s00, best_s01, best_hotels_country, best_hotels_search_dest, best_hotels_od_ulc, best_hotels_uid_miss, popular_hotel_cluster, best_hotels_dates, best_hotels_season):
    # now = datetime.datetime.now()
    # path = 'submission_' + str(now.strftime("%Y-%m-%d-%H-%M")) + '.csv'
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
        filled = []

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
        if s1 in best_hotels_od_ulc:
            d = best_hotels_od_ulc[s1]
            topitems = nlargest(5, sorted(d.items()), key=itemgetter(1))
            for i in range(len(topitems)):
                if topitems[i][0] in filled:
                    continue
                if len(filled) == 5:
                    break
                out.write(' ' + topitems[i][0])
                filled.append(topitems[i][0])
                total1 += 1

        if orig_destination_distance == '':
            s0 = (user_id, user_location_city,
                  srch_destination_id, hotel_country, hotel_market)
            if s0 in best_hotels_uid_miss:
                d = best_hotels_uid_miss[s0]
                topitems = nlargest(4, sorted(d.items()), key=itemgetter(1))
                for i in range(len(topitems)):
                    if topitems[i][0] in filled:
                        continue
                    if len(filled) == 5:
                        break
                    out.write(' ' + topitems[i][0])
                    filled.append(topitems[i][0])
                    total0 += 1

        s00 = (user_id, user_location_city,
               srch_destination_id, hotel_country, hotel_market)
        s01 = (user_id, srch_destination_id, hotel_country, hotel_market)
        if s01 in best_s01 and s00 not in best_s00:
            # print(s01)
            d = best_s01[s01]
            topitems = nlargest(4, sorted(d.items()), key=itemgetter(1))
            for i in range(len(topitems)):
                if topitems[i][0] in filled:
                    continue
                if len(filled) == 5:
                    break
                out.write(' ' + topitems[i][0])
                filled.append(topitems[i][0])
                total00 += 1

        s2 = (srch_destination_id, hotel_country, hotel_market, is_package)
        if s2 in best_hotels_search_dest:
            d = best_hotels_search_dest[s2]
            topitems = nlargest(5, d.items(), key=itemgetter(1))
            for i in range(len(topitems)):
                if topitems[i][0] in filled:
                    continue
                if len(filled) == 5:
                    break
                out.write(' ' + topitems[i][0])
                filled.append(topitems[i][0])
                total2 += 1

        # weekend
        sw = (weekday_booking, stay_duration,
              srch_destination_id, hotel_country, hotel_market)
        if weekday_booking != -1 and stay_duration != -1 and sw in best_hotels_dates:
            d = best_hotels_dates[sw]
            topitems = nlargest(5, d.items(), key=itemgetter(1))
            for i in range(len(topitems)):
                if topitems[i][0] in filled:
                    continue
                if len(filled) == 5:
                    break
                out.write(' ' + topitems[i][0])
                filled.append(topitems[i][0])
                totalw += 1

        # End of mycode

        # season
        ss = (season_booking, srch_destination_id)
        if False:  # srch_destination_id != '' and hotel_country != '' and ss in best_hotels_season:
            d = best_hotels_season[ss]
            topitems = nlargest(5, d.items(), key=itemgetter(1))
            for i in range(len(topitems)):
                if topitems[i][0] in filled:
                    continue
                if len(filled) == 5:
                    break
                out.write(' ' + topitems[i][0])
                filled.append(topitems[i][0])
                totalseason += 1

        s3 = (hotel_market)
        if s3 in best_hotels_country:
            d = best_hotels_country[s3]
            topitems = nlargest(5, d.items(), key=itemgetter(1))
            for i in range(len(topitems)):
                if topitems[i][0] in filled:
                    continue
                if len(filled) == 5:
                    break
                out.write(' ' + topitems[i][0])
                filled.append(topitems[i][0])
                total3 += 1

        for i in range(len(topclasters)):
            if topclasters[i][0] in filled:
                continue
            if len(filled) == 5:
                break
            out.write(' ' + topclasters[i][0])
            filled.append(topclasters[i][0])
            total4 += 1

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


best_s00, best_s01, best_hotels_country, best_hotels_od_ulc, best_hotels_uid_miss, best_hotels_search_dest, popular_hotel_cluster, best_hotels_dates, best_hotels_season = prepare_arrays_match()
gen_submission(best_s00, best_s01, best_hotels_country, best_hotels_search_dest, best_hotels_od_ulc,
               best_hotels_uid_miss, popular_hotel_cluster, best_hotels_dates, best_hotels_season)
