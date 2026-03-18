#!/usr/bin/env python3
"""
360 Feedback Data Processor
Reads 360feedbackresult.csv and question_mapping.csv, produces report_data.json
"""

import csv
import json
from collections import defaultdict

FEEDBACK_CSV = '/Users/nitsir/Downloads/360survey/360feedbackresult.csv'
MAPPING_CSV = '/Users/nitsir/Downloads/360survey/question_mapping.csv'
OUTPUT_JSON = '/Users/nitsir/Downloads/360survey/report_data.json'

# ─────────────────────────────────────────────
# 1. Build question map from question_mapping.csv
#    Line numbering: line 1 = header row, so row i in reader → line (i+2)
#    question_id in data == line number in mapping file
# ─────────────────────────────────────────────

def build_question_map():
    """Returns dict: qid (int) → {theme, topic, question_en, question_th, reverse, section}"""
    with open(MAPPING_CSV, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    qmap = {}
    current_theme = ''
    current_topic = ''

    for i, row in enumerate(rows):
        line_num = i + 2  # line 1 = header
        theme = row.get('Theme', '').strip()
        topic = row.get('Topic', '').strip()
        question_en = row.get('Questions', '').strip()
        question_th = row.get('Thai', '').strip()
        reverse_raw = row.get('Reverse', '').strip().upper()
        reverse = reverse_raw == 'TRUE'
        origin = row.get('Origin', '').strip()

        if theme:
            current_theme = theme
        if topic:
            current_topic = topic

        qmap[line_num] = {
            'theme': current_theme,
            'topic': current_topic,
            'question_en': question_en,
            'question_th': question_th,
            'reverse': reverse,
            'origin': origin,
        }

    return qmap


# ─────────────────────────────────────────────
# 2. Classify each question ID into a section
# ─────────────────────────────────────────────

# CCL Leadership competency question IDs (scored 1-5)
CCL_QIDS = set([
    5,6,7,8,9,10,11,
    23,24,25,26,27,28,
    41,42,43,44,
    45,46,
    60,61,62,63,64,65,66,67,68,
    69,70,71,
    78,79,80,
    81,82,83,84,85,86,
    94,95,96,
    97,98,99,100,101,102,
    103,104,105,106,
    120,121,122,
    123,124,
    130,131,132,
    133,134,
    139,140,141,
    142,143,144,
    145,146,
    151,152,153,
    154,155,156,157,158,
    166,167,168,
])

# Overall Ratings (scored)
OVERALL_RATING_QIDS = set([169, 170, 171, 172])

# Open-ended text Q173-176
OPEN_ENDED_QIDS = set([173, 174, 175, 176])

# Edmondson Team Health (scored, some reverse-coded)
EDMONDSON_QIDS = set([177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191])

# Additional scored items Q192-198 (treat as Team Health)
ADDITIONAL_QIDS = set([192, 193, 194, 195, 196, 197, 198])

# Edmondson dimension grouping (based on question_mapping carry-forward theme)
EDMONDSON_GROUPS = {
    'Psychological Safety': [177, 178, 179, 180],
    'Dependability': [181, 182],
    'Clarity': [183, 184, 185],
    'Meaning': [186, 187, 188],
    'Impact': [189, 190, 191],
}

# Labels for open-ended questions
OPEN_ENDED_LABELS = {
    173: {'en': "Strengths", 'th': "จุดแข็งที่โดดเด่นที่สุดของบุคคลนี้ในฐานะผู้นำคืออะไร?"},
    174: {'en': "Development Needs", 'th': "สิ่งที่บุคคลนี้ควรพัฒนามากที่สุดในฐานะผู้นำคืออะไร?"},
    175: {'en': "Develop Leaders", 'th': "บุคคลนี้สามารถทำอะไรเพิ่มเติมได้อีกบ้าง เพื่อช่วยพัฒนาผู้นำคนอื่น ๆ ในองค์กร?"},
    176: {'en': "Increase Impact", 'th': "สิ่งหนึ่งที่บุคคลนี้ทำแล้วจะช่วยเพิ่มผลกระทบเชิงบวก (Impact) ในฐานะผู้นำได้มากที่สุดคืออะไร?"},
}

# CCL Theme grouping
CCL_THEMES = {
    'Leading Organization': {
        'Strategic Planning': [5, 6, 7, 8, 9],
        'Strategic Perspective': [10, 11],
        'Results Orientation': [23, 24, 25, 26],
        'Decisiveness': [27, 28],
        'Change Management': [41, 42, 43, 44],
    },
    'Leading Others': {
        'Developing and Empowering': [45, 46],
        'Leading Employees': [60, 61, 62, 63, 64, 65, 66, 67, 68],
        'Confronting Problem Employees': [69, 70, 71],
        'Forging Synergy': [78, 79, 80],
        'Participative Management': [81, 82, 83, 84, 85, 86],
        'Building Collaborative Relationships': [94, 95, 96],
        'Communicating Effectively': [97, 98, 99, 100, 101, 102],
        'Interpersonal Savvy': [103, 104, 105, 106],
        'Respect for Differences': [120, 121, 122],
    },
    'Leading Yourself': {
        'Courage': [123, 124],
        'Taking Initiative': [130, 131, 132],
        'Executive Image': [133, 134],
        'Composure': [139, 140, 141],
        'Balance Between Personal and Work Life': [142, 143, 144],
        'Learning from Experience': [145, 146],
        'Self-awareness': [151, 152, 153],
        'Credibility': [154, 155, 156, 157, 158],
        'Career Management': [166, 167, 168],
    },
}


def safe_score(val):
    """Convert score value to float or None."""
    if val is None or val == 'null' or val == '':
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def apply_reverse(score, reverse):
    """Apply reverse coding: score = 6 - raw_score."""
    if score is None:
        return None
    if reverse:
        return 6.0 - score
    return score


def avg(scores):
    """Calculate average of non-None scores."""
    valid = [s for s in scores if s is not None]
    if not valid:
        return None
    return round(sum(valid) / len(valid), 3)


# ─────────────────────────────────────────────
# 3. Process feedback data
# ─────────────────────────────────────────────

def process_data():
    qmap = build_question_map()

    # Read all rows
    with open(FEEDBACK_CSV, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        all_rows = list(reader)

    # Group rows by student
    student_rows = defaultdict(list)
    student_info = {}

    for row in all_rows:
        name = row['student_name']
        student_rows[name].append(row)
        if name not in student_info:
            student_info[name] = {
                'name': name,
                'nickname': row['student_nickname'],
                'email': row['student_email'],
                'level': row['student_level'],
                'role': row['student_role_name'],
                'company': row['student_company'],
            }

    students_output = []

    for name in sorted(student_rows.keys()):
        rows = student_rows[name]
        info = student_info[name]

        # ── Evaluator counts ──
        eval_ids = {
            'higher': set(),
            'equal': set(),
            'lower': set(),
            'self': set(),
        }
        for r in rows:
            ev_id = r['evaluation_id']
            if r['is_self_evaluation'] == 'true':
                eval_ids['self'].add(ev_id)
            else:
                level = r['evaluator_level']
                if level in eval_ids:
                    eval_ids[level].add(ev_id)

        evaluator_counts = {k: len(v) for k, v in eval_ids.items()}

        # ── Per-question scores ──
        # Structure: qid → {others: [scores], self: score, higher: [scores], equal: [scores], lower: [scores]}
        q_scores = defaultdict(lambda: {
            'others': [],
            'self': None,
            'higher': [],
            'equal': [],
            'lower': [],
        })

        for r in rows:
            if r['is_skipped'] == 'true':
                continue
            qid = int(r['question_id'])
            raw = safe_score(r['score'])
            if raw is None:
                continue

            q_meta = qmap.get(qid, {})
            reverse = q_meta.get('reverse', False)
            score = apply_reverse(raw, reverse)

            is_self = r['is_self_evaluation'] == 'true'
            level = r['evaluator_level']

            if is_self:
                q_scores[qid]['self'] = score
            else:
                q_scores[qid]['others'].append(score)
                if level in ('higher', 'equal', 'lower'):
                    q_scores[qid][level].append(score)

        # ── CCL Competency: topic averages ──
        def topic_avgs_for_theme(theme_dict):
            result = {}
            for theme, topics in theme_dict.items():
                result[theme] = {}
                for topic, qids in topics.items():
                    others_scores = []
                    self_scores = []
                    higher_scores = []
                    equal_scores = []
                    lower_scores = []
                    questions = []

                    for qid in qids:
                        if qid not in q_scores:
                            continue
                        qs = q_scores[qid]
                        q_meta = qmap.get(qid, {})

                        q_others_avg = avg(qs['others'])
                        q_self = qs['self']

                        if qs['others']:
                            others_scores.extend(qs['others'])
                        if q_self is not None:
                            self_scores.append(q_self)
                        higher_scores.extend(qs['higher'])
                        equal_scores.extend(qs['equal'])
                        lower_scores.extend(qs['lower'])

                        questions.append({
                            'qid': qid,
                            'question_en': q_meta.get('question_en', ''),
                            'question_th': q_meta.get('question_th', ''),
                            'others_avg': q_others_avg,
                            'self': q_self,
                            'higher_avg': avg(qs['higher']),
                            'equal_avg': avg(qs['equal']),
                            'lower_avg': avg(qs['lower']),
                            'n_others': len(qs['others']),
                        })

                    result[theme][topic] = {
                        'others_avg': avg(others_scores),
                        'self_avg': avg(self_scores),
                        'higher_avg': avg(higher_scores),
                        'equal_avg': avg(equal_scores),
                        'lower_avg': avg(lower_scores),
                        'questions': questions,
                    }
            return result

        ccl_data = topic_avgs_for_theme(CCL_THEMES)

        # ── Theme averages (for radar chart) ──
        theme_avgs = {}
        for theme, topics in CCL_THEMES.items():
            all_others = []
            all_self = []
            for topic, qids in topics.items():
                for qid in qids:
                    if qid not in q_scores:
                        continue
                    qs = q_scores[qid]
                    all_others.extend(qs['others'])
                    if qs['self'] is not None:
                        all_self.append(qs['self'])
            theme_avgs[theme] = {
                'others_avg': avg(all_others),
                'self_avg': avg(all_self),
            }

        # ── Top/Bottom topics ──
        topic_list = []
        for theme, topics in CCL_THEMES.items():
            for topic_name, qids in topics.items():
                td = ccl_data[theme][topic_name]
                topic_list.append({
                    'theme': theme,
                    'topic': topic_name,
                    'others_avg': td['others_avg'],
                    'self_avg': td['self_avg'],
                    'gap': round((td['self_avg'] or 0) - (td['others_avg'] or 0), 3)
                         if td['self_avg'] is not None and td['others_avg'] is not None
                         else None,
                })

        valid_topics = [t for t in topic_list if t['others_avg'] is not None]
        sorted_by_others = sorted(valid_topics, key=lambda x: x['others_avg'], reverse=True)
        top_strengths = sorted_by_others[:5]
        dev_areas = sorted_by_others[-5:][::-1]

        # ── Self-others gap analysis ──
        topics_with_gap = [t for t in valid_topics if t['gap'] is not None]
        blind_spots = sorted(topics_with_gap, key=lambda x: x['gap'])[:5]       # self << others (overestimates)
        hidden_strengths = sorted(topics_with_gap, key=lambda x: x['gap'], reverse=True)[:5]  # self >> others

        # ── Overall Ratings Q169-172 ──
        overall_ratings = []
        for qid in sorted(OVERALL_RATING_QIDS):
            q_meta = qmap.get(qid, {})
            qs = q_scores.get(qid, {'others': [], 'self': None, 'higher': [], 'equal': [], 'lower': []})

            # Score distribution 1-5
            dist = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
            for s in qs['others']:
                k = int(round(s))
                if k in dist:
                    dist[k] += 1

            overall_ratings.append({
                'qid': qid,
                'question_en': q_meta.get('question_en', f'Q{qid}'),
                'question_th': q_meta.get('question_th', ''),
                'others_avg': avg(qs['others']),
                'self': qs['self'],
                'higher_avg': avg(qs['higher']),
                'equal_avg': avg(qs['equal']),
                'lower_avg': avg(qs['lower']),
                'distribution': dist,
                'n_others': len(qs['others']),
            })

        # ── Edmondson Team Health ──
        edmondson_groups = []
        for dim_name, qids in EDMONDSON_GROUPS.items():
            dim_others = []
            dim_higher = []
            dim_equal = []
            dim_lower = []
            dim_self = []
            dim_questions = []
            for qid in qids:
                q_meta = qmap.get(qid, {})
                qs = q_scores.get(qid, {'others': [], 'self': None, 'higher': [], 'equal': [], 'lower': []})

                q_others_avg = avg(qs['others'])
                if qs['others']:
                    dim_others.extend(qs['others'])
                if qs['higher']:
                    dim_higher.extend(qs['higher'])
                if qs['equal']:
                    dim_equal.extend(qs['equal'])
                if qs['lower']:
                    dim_lower.extend(qs['lower'])
                if qs['self'] is not None:
                    dim_self.append(qs['self'])

                dim_questions.append({
                    'qid': qid,
                    'question_th': q_meta.get('question_en', ''),  # Thai stored in 'en' field for Edmondson
                    'others_avg': q_others_avg,
                    'higher_avg': avg(qs['higher']),
                    'equal_avg': avg(qs['equal']),
                    'lower_avg': avg(qs['lower']),
                    'self': qs['self'],
                    'n_others': len(qs['others']),
                    'reverse': q_meta.get('reverse', False),
                })

            edmondson_groups.append({
                'dimension': dim_name,
                'others_avg': avg(dim_others),
                'higher_avg': avg(dim_higher),
                'equal_avg': avg(dim_equal),
                'lower_avg': avg(dim_lower),
                'self_avg': avg(dim_self),
                'questions': dim_questions,
            })

        # Additional Q192-198 as extra team health items
        additional_items = []
        for qid in sorted(ADDITIONAL_QIDS):
            q_meta = qmap.get(qid, {})
            qs = q_scores.get(qid, {'others': [], 'self': None, 'higher': [], 'equal': [], 'lower': []})
            if not qs['others'] and qs['self'] is None:
                continue
            additional_items.append({
                'qid': qid,
                'question_en': q_meta.get('question_en', ''),
                'question_th': q_meta.get('question_th', ''),
                'others_avg': avg(qs['others']),
                'n_others': len(qs['others']),
            })

        # ── Open-ended responses Q173-176 ──
        open_ended = {}
        for r in rows:
            qid_str = r['question_id']
            if not qid_str.isdigit():
                continue
            qid = int(qid_str)
            if qid not in OPEN_ENDED_QIDS:
                continue
            if r['is_skipped'] == 'true':
                continue
            text = r.get('text_response', '')
            if not text or text == 'null' or not text.strip() or text.strip() == '/':
                continue

            level = 'self' if r['is_self_evaluation'] == 'true' else r['evaluator_level']

            if qid not in open_ended:
                open_ended[qid] = {
                    'qid': qid,
                    'label_en': OPEN_ENDED_LABELS.get(qid, {}).get('en', f'Q{qid}'),
                    'label_th': OPEN_ENDED_LABELS.get(qid, {}).get('th', ''),
                    'responses': {'higher': [], 'equal': [], 'lower': [], 'self': []},
                }
            if level in open_ended[qid]['responses']:
                open_ended[qid]['responses'][level].append(text.strip())

        open_ended_list = [open_ended[qid] for qid in sorted(open_ended.keys())]

        # ── Assemble student output ──
        students_output.append({
            'name': info['name'],
            'nickname': info['nickname'],
            'email': info['email'],
            'level': info['level'],
            'role': info['role'],
            'company': info['company'],
            'evaluator_counts': evaluator_counts,
            'theme_averages': theme_avgs,
            'ccl_competencies': ccl_data,
            'top_strengths': top_strengths,
            'development_areas': dev_areas,
            'blind_spots': blind_spots,
            'hidden_strengths': hidden_strengths,
            'overall_ratings': overall_ratings,
            'edmondson': edmondson_groups,
            'additional_items': additional_items,
            'open_ended': open_ended_list,
        })

    output = {
        'meta': {
            'title': 'CCL 360-Degree Leadership Assessment',
            'subtitle': 'Executive Cohort Report',
            'generated': '2026-03-19',
            'total_students': len(students_output),
        },
        'students': students_output,
        'ccl_themes': CCL_THEMES,
        'edmondson_groups': {k: v for k, v in EDMONDSON_GROUPS.items()},
    }

    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"Wrote {OUTPUT_JSON}")
    print(f"Students processed: {len(students_output)}")
    for s in students_output:
        ec = s['evaluator_counts']
        print(f"  {s['name']} ({s['nickname']}): higher={ec['higher']}, equal={ec['equal']}, lower={ec['lower']}, self={ec['self']}")


if __name__ == '__main__':
    process_data()
