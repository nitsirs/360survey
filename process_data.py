#!/usr/bin/env python3
"""
360 Feedback Data Processor
Fetches data from Supabase (or falls back to CSV) and produces report_data.json.
Question IDs are hardcoded from Pete repo's questions-data.ts — NOT derived from CSV row numbers.
"""

import csv
import json
import os
import sys
from collections import defaultdict

# Fix for Anaconda on Windows: ensure SSL DLLs are on PATH
_anaconda_lib = r'C:\ProgramData\Anaconda3\Library\bin'
if os.path.isdir(_anaconda_lib) and _anaconda_lib not in os.environ.get('PATH', ''):
    os.environ['PATH'] = _anaconda_lib + os.pathsep + os.environ.get('PATH', '')

# ─────────────────────────────────────────────
# CONFIGURATION — fill in your Supabase credentials
# ─────────────────────────────────────────────

SUPABASE_URL      = 'https://iuxgoprnsnluwlwcbcqc.supabase.co'
SUPABASE_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Iml1eGdvcHJuc25sdXdsd2NiY3FjIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njk1MzIyMTMsImV4cCI6MjA4NTEwODIxM30.BF1PbYVYDlFiAgC665cZ8lumaOn35xNr2PnFyMUW7t0'

SESSION_ID = 'f8a25b1f-4495-41a4-a40b-2abfe8e9535a'

# Fallback CSV path (used if Supabase credentials are not set)
FEEDBACK_CSV = 'C:/Users/User/Downloads/360survey/360feedbackresult.csv'

OUTPUT_JSON  = 'C:/Users/User/Downloads/360survey/report_data.json'


# ─────────────────────────────────────────────
# 1. QUESTION DATA — hardcoded from Pete repo's questions-data.ts
#    These IDs match exactly what is stored in responses.question_id in the DB.
#    Do NOT derive from CSV line numbers.
# ─────────────────────────────────────────────

# reverse=True means score should be reverse-coded (6 - score) when computing averages
QUESTION_DATA = {
    # ── Leading Organization: Strategic Planning (executive) ──────────────────
    5:  {'theme': 'Leading Organization', 'topic': 'Strategic Planning', 'target': 'executive', 'reverse': False,
         'text_en': 'Regularly updates plans to reflect changing circumstances.',
         'text_th': 'คอยอัปเดตแผนงานให้ทันต่อสถานการณ์ที่เปลี่ยนแปลงอยู่เสมอ'},
    6:  {'theme': 'Leading Organization', 'topic': 'Strategic Planning', 'target': 'executive', 'reverse': False,
         'text_en': 'Translates his or her vision into realistic business strategies.',
         'text_th': 'สามารถเปลี่ยนวิสัยทัศน์ (Vision) ให้กลายเป็นกลยุทธ์ที่ทำได้จริง'},
    7:  {'theme': 'Leading Organization', 'topic': 'Strategic Planning', 'target': 'executive', 'reverse': False,
         'text_en': 'Weighs the concerns of all relevant business functions when developing plans.',
         'text_th': 'รับฟังและพิจารณาข้อกังวลของทุกฝ่ายที่เกี่ยวข้องในการวางแผนงาน'},
    8:  {'theme': 'Leading Organization', 'topic': 'Strategic Planning', 'target': 'executive', 'reverse': False,
         'text_en': 'Articulates wise, long-term objectives and strategies.',
         'text_th': 'สามารถถ่ายทอดเป้าหมายและกลยุทธ์ระยะยาวได้อย่างชาญฉลาดและเห็นภาพ'},
    9:  {'theme': 'Leading Organization', 'topic': 'Strategic Planning', 'target': 'executive', 'reverse': False,
         'text_en': 'Develops plans that balance long-term goals with immediate organizational needs.',
         'text_th': 'วางแผนโดยรักษาสมดุลระหว่างเป้าหมายระยะยาวและความต้องการเร่งด่วนขององค์กร'},
    10: {'theme': 'Leading Organization', 'topic': 'Strategic Planning', 'target': 'executive', 'reverse': False,
         'text_en': 'Develops plans that contain contingencies for future changes.',
         'text_th': 'เตรียมแผนสำรองไว้ล่วงหน้าเพื่อรองรับการเปลี่ยนแปลงในอนาคต'},
    11: {'theme': 'Leading Organization', 'topic': 'Strategic Planning', 'target': 'executive', 'reverse': False,
         'text_en': 'Successfully integrates strategic and tactical planning.',
         'text_th': 'สามารถเชื่อมแผนกลยุทธ์ (แผนระยะยาว) เข้ากับแผนปฏิบัติการ (แผนระยะกลาง) ได้อย่างลงตัวและมีประสิทธิภาพ'},

    # ── Leading Organization: Strategic Perspective (mid_senior) ─────────────
    12: {'theme': 'Leading Organization', 'topic': 'Strategic Perspective', 'target': 'mid_senior', 'reverse': False,
         'text_en': 'Does his/her homework before making a proposal to top management.',
         'text_th': 'เตรียมตัวและทำการบ้านมาอย่างดีก่อนนำเสนองานต่อผู้บริหารระดับสูง'},
    13: {'theme': 'Leading Organization', 'topic': 'Strategic Perspective', 'target': 'mid_senior', 'reverse': False,
         'text_en': 'Works effectively with higher management (e.g., presents to them, persuades them, and stands up to them if necessary).',
         'text_th': 'ทำงานร่วมกับผู้บริหารระดับสูงได้อย่างมีประสิทธิภาพ (เช่น กล้านำเสนอ โน้มน้าว และยืนหยัดในสิ่งที่ถูกต้อง)'},
    14: {'theme': 'Leading Organization', 'topic': 'Strategic Perspective', 'target': 'mid_senior', 'reverse': False,
         'text_en': 'Links his/her responsibilities with the mission of the whole organization.',
         'text_th': 'เชื่อมโยงบทบาทความรับผิดชอบของตนเองเข้ากับพันธกิจขององค์กรได้ชัดเจน'},
    15: {'theme': 'Leading Organization', 'topic': 'Strategic Perspective', 'target': 'mid_senior', 'reverse': False,
         'text_en': 'Once the more glaring problems in an assignment are solved, can see the underlying problems and patterns that were obscured before.',
         'text_th': 'เมื่อแก้ปัญหาที่เห็นชัดๆ ในงานไปแล้ว สามารถมองเห็นสาเหตุที่แท้จริงและรูปแบบของปัญหาที่ซ่อนอยู่เบื้องลึกได้'},
    16: {'theme': 'Leading Organization', 'topic': 'Strategic Perspective', 'target': 'mid_senior', 'reverse': False,
         'text_en': 'Understands higher management values, how higher management operates, and how they see things.',
         'text_th': 'เข้าใจค่านิยม วิธีการทำงาน และมุมมองของผู้บริหารระดับสูงเป็นอย่างดี'},
    17: {'theme': 'Leading Organization', 'topic': 'Strategic Perspective', 'target': 'mid_senior', 'reverse': False,
         'text_en': 'Analyzes a complex situation carefully, then reduces it to its simplest terms in searching for a solution.',
         'text_th': 'วิเคราะห์สถานการณ์ที่ซับซ้อนได้อย่างละเอียด และย่อยให้เข้าใจง่ายเพื่อหาทางแก้ไข'},
    18: {'theme': 'Leading Organization', 'topic': 'Strategic Perspective', 'target': 'mid_senior', 'reverse': False,
         'text_en': 'Learns from the mistakes of higher management (i.e., does not repeat them him/herself).',
         'text_th': 'เรียนรู้จากข้อผิดพลาดของฝ่ายบริหารระดับสูง (เพื่อไม่ให้เกิดความผิดพลาดซ้ำรอย)'},
    19: {'theme': 'Leading Organization', 'topic': 'Strategic Perspective', 'target': 'mid_senior', 'reverse': False,
         'text_en': 'Has solid working relationships with higher management.',
         'text_th': 'มีความสัมพันธ์ในการทำงานที่แน่นแฟ้นกับผู้บริหารระดับสูง'},

    # ── Leading Organization: Results Orientation (executive) ─────────────────
    23: {'theme': 'Leading Organization', 'topic': 'Results Orientation', 'target': 'executive', 'reverse': False,
         'text_en': 'Assigns clear accountability for important objectives.',
         'text_th': 'มอบหมายและกำหนดผู้รับผิดชอบในเป้าหมายสำคัญต่างๆ ได้อย่างชัดเจน'},
    24: {'theme': 'Leading Organization', 'topic': 'Results Orientation', 'target': 'executive', 'reverse': False,
         'text_en': 'Pushes the organization to address the concerns of key stakeholders.',
         'text_th': 'ผลักดันองค์กรให้รับมือกับความกังวลของผู้มีส่วนได้ส่วนเสียและฝ่ายสำคัญที่เกี่ยวข้อง'},
    25: {'theme': 'Leading Organization', 'topic': 'Results Orientation', 'target': 'executive', 'reverse': False,
         'text_en': 'Clearly conveys objectives, deadlines, and expectations.',
         'text_th': 'สื่อสารเป้าหมาย กำหนดกรอบเวลา และความคาดหวังอย่างชัดเจน'},
    26: {'theme': 'Leading Organization', 'topic': 'Results Orientation', 'target': 'executive', 'reverse': False,
         'text_en': 'Holds self accountable for meeting commitments.',
         'text_th': 'ยึดมั่นและรับผิดชอบต่อคำมั่นสัญญาที่ให้ไว้'},
    27: {'theme': 'Leading Organization', 'topic': 'Results Orientation', 'target': 'executive', 'reverse': False,
         'text_en': 'Aligns organizational resources to accomplish key objectives.',
         'text_th': 'จัดสรรทรัพยากรขององค์กรให้สอดคล้องกับการบรรลุเป้าหมายสำคัญ'},
    28: {'theme': 'Leading Organization', 'topic': 'Results Orientation', 'target': 'executive', 'reverse': False,
         'text_en': 'Acts with a sense of urgency.',
         'text_th': 'ปฏิบัติงานด้วยความกระตือรือร้นและรวดเร็ว'},

    # ── Leading Organization: Decisiveness (mid_senior) ──────────────────────
    29: {'theme': 'Leading Organization', 'topic': 'Decisiveness', 'target': 'mid_senior', 'reverse': False,
         'text_en': 'Does not hesitate when making decisions.',
         'text_th': 'ตัดสินใจได้ทันท่วงที ไม่ลังเลใจ'},
    30: {'theme': 'Leading Organization', 'topic': 'Decisiveness', 'target': 'mid_senior', 'reverse': False,
         'text_en': 'Does not become paralyzed or overwhelmed when facing action.',
         'text_th': 'ไม่หยุดชะงักหรือลนลานเมื่อต้องเผชิญหน้ากับสถานการณ์ที่ต้องลงมือทำทันที'},
    31: {'theme': 'Leading Organization', 'topic': 'Decisiveness', 'target': 'mid_senior', 'reverse': False,
         'text_en': 'Is action-oriented.',
         'text_th': 'เน้นการลงมือปฏิบัติจริง (Action-oriented)'},

    # ── Leading Others: Developing and Empowering (executive) ────────────────
    41: {'theme': 'Leading Others', 'topic': 'Developing and Empowering', 'target': 'executive', 'reverse': False,
         'text_en': 'Delegates work that provides substantial responsibility and visibility.',
         'text_th': 'มอบหมายงานที่ท้าทายและช่วยให้ทีมงานได้มีโอกาสแสดงฝีมือ'},
    42: {'theme': 'Leading Others', 'topic': 'Developing and Empowering', 'target': 'executive', 'reverse': False,
         'text_en': 'Acts as a mentor, helping others to develop and advance in their careers.',
         'text_th': 'ทำหน้าที่เป็นพี่เลี้ยง ช่วยพัฒนาและสนับสนุนความก้าวหน้าในอาชีพของผู้อื่น'},
    43: {'theme': 'Leading Others', 'topic': 'Developing and Empowering', 'target': 'executive', 'reverse': False,
         'text_en': 'Supports the decisions and actions of direct reports.',
         'text_th': 'สนับสนุนการตัดสินใจและการกระทำของลูกน้อง'},
    44: {'theme': 'Leading Others', 'topic': 'Developing and Empowering', 'target': 'executive', 'reverse': False,
         'text_en': "Utilizes others' capabilities appropriately.",
         'text_th': 'ดึงเอาความสามารถและศักยภาพของคนอื่นมาใช้ได้อย่างเหมาะสมกับงาน'},
    45: {'theme': 'Leading Others', 'topic': 'Developing and Empowering', 'target': 'executive', 'reverse': False,
         'text_en': 'Develops staff through constructive feedback and encouragement.',
         'text_th': 'พัฒนาพนักงานผ่านการให้ฟีดแบ็ก (Feedback) เชิงสร้างสรรค์และการให้กำลังใจ'},
    46: {'theme': 'Leading Others', 'topic': 'Developing and Empowering', 'target': 'executive', 'reverse': False,
         'text_en': 'Encourages individual initiative in determining how to achieve broad goals.',
         'text_th': 'ส่งเสริมให้ทีมงานคิดริเริ่มหาวิธีบรรลุเป้าหมายใหญ่ๆ ด้วยตนเอง'},

    # ── Leading Others: Leading Employees (mid_senior) ───────────────────────
    47: {'theme': 'Leading Others', 'topic': 'Leading Employees', 'target': 'mid_senior', 'reverse': False,
         'text_en': "Is willing to delegate important tasks, not just things he/she doesn't want to do.",
         'text_th': 'พร้อมมอบหมายงานที่สำคัญ ไม่ใช่แค่โยนงานที่ตัวเองไม่อยากทำ'},
    48: {'theme': 'Leading Others', 'topic': 'Leading Employees', 'target': 'mid_senior', 'reverse': False,
         'text_en': 'Provides prompt feedback, both positive and negative.',
         'text_th': 'ให้ฟีดแบ็ก (Feedback) อย่างรวดเร็วและสม่ำเสมอ ทั้งคำชมและสิ่งที่ต้องปรับปรุง'},
    49: {'theme': 'Leading Others', 'topic': 'Leading Employees', 'target': 'mid_senior', 'reverse': False,
         'text_en': "Pushes decision making to the lowest appropriate level and develops employees' confidence in their ability to make those decisions.",
         'text_th': 'กระจายอำนาจการตัดสินใจลงไปสู่ระดับที่เหมาะสม และสร้างความมั่นใจให้ลูกน้องกล้าตัดสินใจเอง'},
    50: {'theme': 'Leading Others', 'topic': 'Leading Employees', 'target': 'mid_senior', 'reverse': False,
         'text_en': 'Acts fairly and does not play favorites.',
         'text_th': 'ปฏิบัติต่อทุกคนอย่างเป็นธรรม และไม่เลือกที่รักมักที่ชัง'},
    51: {'theme': 'Leading Others', 'topic': 'Leading Employees', 'target': 'mid_senior', 'reverse': False,
         'text_en': "Uses his/her knowledge base to broaden the range of problem-solving options for direct reports to take.",
         'text_th': 'ใช้ความรู้และประสบการณ์ที่มี ช่วยขยายมุมมองและเพิ่มทางเลือกในการแก้ปัญหาให้กับลูกน้อง'},
    52: {'theme': 'Leading Others', 'topic': 'Leading Employees', 'target': 'mid_senior', 'reverse': False,
         'text_en': 'In implementing a change, explains, answers questions, and patiently listens to concerns.',
         'text_th': 'เมื่อต้องลงมือเปลี่ยนแปลงอะไรสักอย่าง จะอธิบาย ตอบคำถาม และรับฟังข้อกังวลอย่างอดทน'},
    53: {'theme': 'Leading Others', 'topic': 'Leading Employees', 'target': 'mid_senior', 'reverse': False,
         'text_en': 'Interacts with staff in a way that results in the staff feeling motivated.',
         'text_th': 'มีศิลปะในการเข้าหาลูกน้อง ทำให้ทีมงานรู้สึกมีแรงจูงใจและมีไฟในการทำงาน'},
    54: {'theme': 'Leading Others', 'topic': 'Leading Employees', 'target': 'mid_senior', 'reverse': False,
         'text_en': 'Actively promotes his/her direct reports to senior management.',
         'text_th': 'ผลักดันและนำเสนอศักยภาพของลูกน้องให้ผู้บริหารระดับสูงเห็นความสามารถอยู่เสมอ'},
    55: {'theme': 'Leading Others', 'topic': 'Leading Employees', 'target': 'mid_senior', 'reverse': False,
         'text_en': 'Develops employees by providing challenge and opportunity.',
         'text_th': 'พัฒนาศักยภาพพนักงานโดยการมอบหมายงานที่ท้าทายและสร้างโอกาสใหม่ๆ'},
    56: {'theme': 'Leading Others', 'topic': 'Leading Employees', 'target': 'mid_senior', 'reverse': False,
         'text_en': 'Sets a challenging climate to encourage individual growth.',
         'text_th': 'สร้างสภาพแวดล้อมการทำงานที่ท้าทาย เพื่อกระตุ้นให้พนักงานเกิดการพัฒนาตัวเอง'},
    57: {'theme': 'Leading Others', 'topic': 'Leading Employees', 'target': 'mid_senior', 'reverse': False,
         'text_en': 'Rewards hard work and dedication to excellence.',
         'text_th': 'ให้รางวัลคนที่มีความทุ่มเทและมุ่งมั่นที่จะทำงานให้ดีเยี่ยม'},
    58: {'theme': 'Leading Others', 'topic': 'Leading Employees', 'target': 'mid_senior', 'reverse': False,
         'text_en': 'Surrounds him/herself with the best people.',
         'text_th': 'เลือกทำงานและรายล้อมตัวเองด้วยคนที่มีศักยภาพสูง'},
    59: {'theme': 'Leading Others', 'topic': 'Leading Employees', 'target': 'mid_senior', 'reverse': False,
         'text_en': 'Finds and attracts highly talented and productive people.',
         'text_th': 'สรรหาและดึงดูดคนเก่งที่มีศักยภาพเข้ามาร่วมงาน'},

    # ── Leading Others: Confronting Problem Employees (both) ─────────────────
    60: {'theme': 'Leading Others', 'topic': 'Confronting Problem Employees', 'target': 'both', 'reverse': False,
         'text_en': 'Can deal effectively with resistant employees.',
         'text_th': 'รับมือกับพนักงานที่มีท่าทีต่อต้านได้อย่างมีประสิทธิภาพ'},
    61: {'theme': 'Leading Others', 'topic': 'Confronting Problem Employees', 'target': 'both', 'reverse': False,
         'text_en': 'Acts decisively when faced with a tough decision such as laying off workers, even though it hurts him/her personally.',
         'text_th': 'ตัดสินใจอย่างเด็ดขาดในเรื่องยากๆ เช่น การเลิกจ้าง แม้จะรู้สึกลำบากใจเป็นการส่วนตัว'},
    62: {'theme': 'Leading Others', 'topic': 'Confronting Problem Employees', 'target': 'both', 'reverse': False,
         'text_en': 'Moves quickly in confronting a problem employee.',
         'text_th': 'ดำเนินการกับพนักงานที่มีปัญหาอย่างทันท่วงที'},
    63: {'theme': 'Leading Others', 'topic': 'Confronting Problem Employees', 'target': 'both', 'reverse': False,
         'text_en': 'Is able to fire or deal firmly with loyal but incompetent people without procrastinating.',
         'text_th': 'กล้าให้ออกหรือจัดการกับคนที่ภักดี แต่ไม่มีความสามารถ โดยไม่ผัดวันประกันพรุ่ง'},
    64: {'theme': 'Leading Others', 'topic': 'Confronting Problem Employees', 'target': 'both', 'reverse': False,
         'text_en': 'Correctly identifies potential performance problems early.',
         'text_th': 'มองเห็นได้ตั้งแต่เนิ่นๆ ว่าพนักงานกำลังจะมีปัญหาเรื่องการทำงาน'},
    65: {'theme': 'Leading Others', 'topic': 'Confronting Problem Employees', 'target': 'both', 'reverse': False,
         'text_en': 'Appropriately documents employee performance problems.',
         'text_th': 'บันทึกปัญหาการทำงานของพนักงานเป็นลายลักษณ์อักษรอย่างเหมาะสม'},

    # ── Leading Others: Forging Synergy (executive) ───────────────────────────
    66: {'theme': 'Leading Others', 'topic': 'Forging Synergy', 'target': 'executive', 'reverse': False,
         'text_en': "Focuses others' energy on common goals, priorities, and problems.",
         'text_th': 'ระดมพลังของทุกคนให้มุ่งไปที่เป้าหมายและลำดับความสำคัญเดียวกัน'},
    67: {'theme': 'Leading Others', 'topic': 'Forging Synergy', 'target': 'executive', 'reverse': False,
         'text_en': 'Helps direct reports resolve their conflicts constructively.',
         'text_th': 'ช่วยให้ลูกน้องหาทางออกและแก้ไขความขัดแย้งได้อย่างสร้างสรรค์'},
    68: {'theme': 'Leading Others', 'topic': 'Forging Synergy', 'target': 'executive', 'reverse': False,
         'text_en': 'Seeks common ground in an effort to resolve conflicts.',
         'text_th': 'พยายามแสวงหาจุดร่วม (Common Ground) เพื่อยุติความขัดแย้งที่เกิดขึ้น'},
    69: {'theme': 'Leading Others', 'topic': 'Forging Synergy', 'target': 'executive', 'reverse': False,
         'text_en': 'Works harmoniously with key stakeholders.',
         'text_th': 'ทำงานร่วมกับฝ่ายที่เกี่ยวข้องและผู้มีส่วนได้ส่วนเสียคนสำคัญ ได้อย่างราบรื่น'},
    70: {'theme': 'Leading Others', 'topic': 'Forging Synergy', 'target': 'executive', 'reverse': False,
         'text_en': 'Identifies and removes barriers to effective teamwork.',
         'text_th': 'ระบุและขจัดอุปสรรคที่ขัดขวางการทำงานเป็นทีม'},
    71: {'theme': 'Leading Others', 'topic': 'Forging Synergy', 'target': 'executive', 'reverse': False,
         'text_en': 'Maintains smooth, effective working relationships.',
         'text_th': 'รักษาความสัมพันธ์ในการทำงานที่ดีและราบรื่นกับทุกฝ่าย'},

    # ── Leading Others: Participative Management (both) ──────────────────────
    78: {'theme': 'Leading Others', 'topic': 'Participative Management', 'target': 'both', 'reverse': False,
         'text_en': 'Uses effective listening skills to gain clarification from others.',
         'text_th': 'ใช้ทักษะการฟังที่ดีเพื่อทำความเข้าใจผู้อื่นให้ชัดเจน'},
    79: {'theme': 'Leading Others', 'topic': 'Participative Management', 'target': 'both', 'reverse': False,
         'text_en': 'Is open to the input of others.',
         'text_th': 'เปิดรับความคิดเห็นและมุมมองจากผู้อื่นเสมอ'},
    80: {'theme': 'Leading Others', 'topic': 'Participative Management', 'target': 'both', 'reverse': False,
         'text_en': 'Encourages direct reports to share.',
         'text_th': 'ส่งเสริมให้ลูกน้องกล้าแสดงความคิดเห็น'},
    81: {'theme': 'Leading Others', 'topic': 'Participative Management', 'target': 'both', 'reverse': False,
         'text_en': 'Involves others in the beginning stages of an initiative.',
         'text_th': 'ให้คนในทีมมีส่วนร่วมตั้งแต่ช่วงเริ่มต้นโครงการ'},
    82: {'theme': 'Leading Others', 'topic': 'Participative Management', 'target': 'both', 'reverse': False,
         'text_en': 'Gains commitment of others before implementing changes.',
         'text_th': 'ทำให้ทุกคนเห็นพ้องและพร้อมใจร่วมกัน ก่อนจะลงมือเปลี่ยนแปลงอะไรสักอย่าง'},
    83: {'theme': 'Leading Others', 'topic': 'Participative Management', 'target': 'both', 'reverse': False,
         'text_en': 'Listens to individuals at all levels in the organization.',
         'text_th': 'รับฟังพนักงานทุกระดับในองค์กร'},
    84: {'theme': 'Leading Others', 'topic': 'Participative Management', 'target': 'both', 'reverse': False,
         'text_en': 'Keeps individuals informed of future changes that may impact them.',
         'text_th': 'แจ้งให้คนในทีมทราบล่วงหน้าเกี่ยวกับการเปลี่ยนแปลงที่อาจกระทบพวกเขา'},
    85: {'theme': 'Leading Others', 'topic': 'Participative Management', 'target': 'both', 'reverse': False,
         'text_en': 'Listens to employees both when things are going well and when they are not.',
         'text_th': 'รับฟังพนักงานทั้งในยามที่งานราบรื่นและยามมีปัญหา'},
    86: {'theme': 'Leading Others', 'topic': 'Participative Management', 'target': 'both', 'reverse': False,
         'text_en': 'Involves others before developing plan of action.',
         'text_th': 'ดึงคนที่เกี่ยวข้องเข้ามาร่วมวางแผนตั้งแต่ต้น'},

    # ── Leading Others: Building Collaborative Relationships (mid_senior) ─────
    87: {'theme': 'Leading Others', 'topic': 'Building Collaborative Relationships', 'target': 'mid_senior', 'reverse': False,
         'text_en': 'Gets things done without creating unnecessary adversarial relationships.',
         'text_th': 'ทำงานให้สำเร็จได้โดยไม่สร้างความขัดแย้งหรือศัตรูโดยไม่จำเป็น'},
    88: {'theme': 'Leading Others', 'topic': 'Building Collaborative Relationships', 'target': 'mid_senior', 'reverse': False,
         'text_en': "Uses good timing and common sense in negotiating; makes his/her points when the time is ripe and does it diplomatically.",
         'text_th': 'มีจังหวะและไหวพริบในการเจรจาต่อรอง (พูดถูกที่ ถูกเวลา และนุ่มนวล)'},
    89: {'theme': 'Leading Others', 'topic': 'Building Collaborative Relationships', 'target': 'mid_senior', 'reverse': False,
         'text_en': 'When working with a group over whom he/she has no control, gets things done by finding common ground.',
         'text_th': 'เมื่อต้องทำงานกับกลุ่มคนที่ไม่ได้มีอำนาจสั่งการ สามารถหาทางที่ทุกฝ่ายพอใจ เพื่อผลักดันงานให้สำเร็จได้'},
    90: {'theme': 'Leading Others', 'topic': 'Building Collaborative Relationships', 'target': 'mid_senior', 'reverse': False,
         'text_en': 'When working with peers from other functions or units, gains their cooperation and support.',
         'text_th': 'ได้รับความร่วมมือและการสนับสนุนเมื่อทำงานร่วมกับเพื่อนร่วมงานต่างแผนก'},
    91: {'theme': 'Leading Others', 'topic': 'Building Collaborative Relationships', 'target': 'mid_senior', 'reverse': False,
         'text_en': 'Tries to understand what other people think before making judgments about them.',
         'text_th': 'พยายามทำความเข้าใจมุมมองของคนอื่นก่อนที่จะตัดสินใคร'},
    92: {'theme': 'Leading Others', 'topic': 'Building Collaborative Relationships', 'target': 'mid_senior', 'reverse': False,
         'text_en': 'Quickly gains trust and respect from his/her customers.',
         'text_th': 'สร้างความไว้วางใจและการยอมรับจากลูกค้าได้อย่างรวดเร็ว'},
    93: {'theme': 'Leading Others', 'topic': 'Building Collaborative Relationships', 'target': 'mid_senior', 'reverse': False,
         'text_en': 'Can settle problems with external groups without alienating them.',
         'text_th': 'แก้ปัญหากับคนภายนอกองค์กรได้โดยไม่ให้เกิดความบาดหมาง'},

    # ── Leading Others: Communicating Effectively (both) ─────────────────────
    94: {'theme': 'Leading Others', 'topic': 'Communicating Effectively', 'target': 'both', 'reverse': False,
         'text_en': 'Expresses ideas fluently and eloquently.',
         'text_th': 'ถ่ายทอดความคิดได้อย่างลื่นไหลและมีวาทศิลป์'},
    95: {'theme': 'Leading Others', 'topic': 'Communicating Effectively', 'target': 'both', 'reverse': False,
         'text_en': 'Prevents unpleasant surprises by communicating important information.',
         'text_th': 'สื่อสารข้อมูลสำคัญอย่างสม่ำเสมอ ไม่มีเรื่องให้เซอร์ไพรส์'},
    96: {'theme': 'Leading Others', 'topic': 'Communicating Effectively', 'target': 'both', 'reverse': False,
         'text_en': 'Encourages direct and open discussions about important issues.',
         'text_th': 'ส่งเสริมให้มีการพูดคุยเรื่องสำคัญกันอย่างเปิดเผยและตรงไปตรงมา'},
    97: {'theme': 'Leading Others', 'topic': 'Communicating Effectively', 'target': 'both', 'reverse': False,
         'text_en': 'Writes clearly and concisely.',
         'text_th': 'เขียนสื่อสารได้กระชับ ชัดเจน'},
    98: {'theme': 'Leading Others', 'topic': 'Communicating Effectively', 'target': 'both', 'reverse': False,
         'text_en': 'Conveys ideas through lively examples and images.',
         'text_th': 'ถ่ายทอดไอเดียได้ชัดเจน ผ่านการยกตัวอย่างและเปรียบเทียบให้เห็นภาพ'},
    99: {'theme': 'Leading Others', 'topic': 'Communicating Effectively', 'target': 'both', 'reverse': False,
         'text_en': 'Clearly articulates even the most complex concepts.',
         'text_th': 'สามารถอธิบายแนวคิดที่ซับซ้อนให้เข้าใจง่ายและชัดเจน'},

    # ── Leading Others: Interpersonal Savvy (executive) ──────────────────────
    100: {'theme': 'Leading Others', 'topic': 'Interpersonal Savvy', 'target': 'executive', 'reverse': False,
          'text_en': "Tailors communication based on other's needs, motivations, and agendas.",
          'text_th': 'เลือกวิธีสื่อสารที่เข้ากับความต้องการและแรงจูงใจของผู้ฟัง'},
    101: {'theme': 'Leading Others', 'topic': 'Interpersonal Savvy', 'target': 'executive', 'reverse': False,
          'text_en': 'Understands own impact on situations and people.',
          'text_th': 'รู้ตัวว่าสิ่งที่ทำและพูดส่งผลต่อคนรอบข้างและสถานการณ์อย่างไร'},
    102: {'theme': 'Leading Others', 'topic': 'Interpersonal Savvy', 'target': 'executive', 'reverse': False,
          'text_en': 'Influences others without using formal authority.',
          'text_th': 'สามารถโน้มน้าวผู้อื่นได้โดยไม่ต้องใช้อำนาจสั่งการ'},
    103: {'theme': 'Leading Others', 'topic': 'Interpersonal Savvy', 'target': 'executive', 'reverse': False,
          'text_en': 'Knows when and with whom to build alliances.',
          'text_th': 'รู้ว่าควรสร้างพันธมิตรกับใครและเมื่อไหร่'},
    104: {'theme': 'Leading Others', 'topic': 'Interpersonal Savvy', 'target': 'executive', 'reverse': False,
          'text_en': 'Wins concessions from others without harming relationships.',
          'text_th': 'เจรจาต่อรองให้ได้สิ่งที่ต้องการโดยไม่ทำลายความสัมพันธ์'},
    105: {'theme': 'Leading Others', 'topic': 'Interpersonal Savvy', 'target': 'executive', 'reverse': False,
          'text_en': 'Adjusts leadership style according to the demands of the situation.',
          'text_th': 'ปรับสไตล์การเป็นผู้นำให้เหมาะกับสถานการณ์'},
    106: {'theme': 'Leading Others', 'topic': 'Interpersonal Savvy', 'target': 'executive', 'reverse': False,
          'text_en': 'Accurately senses when to give and take when negotiating.',
          'text_th': 'มีเซนส์ที่แม่นยำว่าควรเป็นฝ่ายรุกหรือฝ่ายรับในการเจรจาต่อรอง'},

    # ── Leading Others: Compassion and Sensitivity (mid_senior) ──────────────
    107: {'theme': 'Leading Others', 'topic': 'Compassion and Sensitivity', 'target': 'mid_senior', 'reverse': False,
          'text_en': 'Is sensitive to signs of overwork in others.',
          'text_th': 'สังเกตเห็นเมื่อคนในทีมเริ่มทำงานหนักเกินไป'},
    108: {'theme': 'Leading Others', 'topic': 'Compassion and Sensitivity', 'target': 'mid_senior', 'reverse': False,
          'text_en': 'Is willing to help an employee with personal problems.',
          'text_th': 'เต็มใจช่วยเหลือพนักงานเมื่อเขามีปัญหาส่วนตัว'},
    109: {'theme': 'Leading Others', 'topic': 'Compassion and Sensitivity', 'target': 'mid_senior', 'reverse': False,
          'text_en': 'Is calm and patient when other people have to miss work due to sick days.',
          'text_th': 'ใจเย็นและเข้าใจเมื่อพนักงานจำเป็นต้องลางานเนื่องจากอาการเจ็บป่วย'},
    110: {'theme': 'Leading Others', 'topic': 'Compassion and Sensitivity', 'target': 'mid_senior', 'reverse': False,
          'text_en': 'Allows new people in a job sufficient time to learn.',
          'text_th': 'ให้เวลาพนักงานใหม่ได้เรียนรู้งานอย่างเพียงพอ'},
    111: {'theme': 'Leading Others', 'topic': 'Compassion and Sensitivity', 'target': 'mid_senior', 'reverse': False,
          'text_en': 'Helps people learn from their mistakes.',
          'text_th': 'ช่วยให้คนในทีมเรียนรู้จากความผิดพลาด'},
    112: {'theme': 'Leading Others', 'topic': 'Compassion and Sensitivity', 'target': 'mid_senior', 'reverse': False,
          'text_en': 'Conveys compassion toward them when other people disclose a personal loss.',
          'text_th': 'แสดงความเห็นใจเมื่อผู้อื่นสูญเสียหรือมีเรื่องกระทบจิตใจ'},

    # ── Leading Others: Putting People at Ease (mid_senior) ──────────────────
    113: {'theme': 'Leading Others', 'topic': 'Putting People at Ease', 'target': 'mid_senior', 'reverse': False,
          'text_en': 'Has a pleasant disposition.',
          'text_th': 'มีอัธยาศัยดี'},
    114: {'theme': 'Leading Others', 'topic': 'Putting People at Ease', 'target': 'mid_senior', 'reverse': False,
          'text_en': 'Has a good sense of humor.',
          'text_th': 'มีอารมณ์ขัน'},
    115: {'theme': 'Leading Others', 'topic': 'Putting People at Ease', 'target': 'mid_senior', 'reverse': False,
          'text_en': 'Has personal warmth.',
          'text_th': 'มีความอบอุ่น เป็นมิตร'},

    # ── Leading Yourself: Courage (both) ─────────────────────────────────────
    120: {'theme': 'Leading Yourself', 'topic': 'Courage', 'target': 'both', 'reverse': False,
          'text_en': 'Takes the lead on unpopular though necessary actions.',
          'text_th': 'กล้าเป็นผู้นำในการทำสิ่งที่จำเป็น แม้สิ่งนั้นอาจจะไม่ถูกใจคนส่วนใหญ่ก็ตาม'},
    121: {'theme': 'Leading Yourself', 'topic': 'Courage', 'target': 'both', 'reverse': False,
          'text_en': 'Acts decisively to tackle difficult problems.',
          'text_th': 'จัดการกับปัญหายากๆ อย่างเด็ดขาด'},
    122: {'theme': 'Leading Yourself', 'topic': 'Courage', 'target': 'both', 'reverse': False,
          'text_en': 'Perseveres in the face of problems and difficulties.',
          'text_th': 'มีความเพียรพยายาม ไม่ย่อท้อต่อปัญหาและอุปสรรค'},
    123: {'theme': 'Leading Yourself', 'topic': 'Courage', 'target': 'both', 'reverse': False,
          'text_en': 'Confronts conflicts promptly so they do not escalate.',
          'text_th': 'รีบจัดการความขัดแย้งทันทีเพื่อไม่ให้ปัญหาบานปลาย'},
    124: {'theme': 'Leading Yourself', 'topic': 'Courage', 'target': 'both', 'reverse': False,
          'text_en': 'Has the courage to confront others when necessary.',
          'text_th': 'มีความกล้าที่จะเผชิญหน้ากับผู้อื่นเมื่อถึงคราวจำเป็น'},

    # ── Leading Yourself: Taking Initiative (mid_senior) ─────────────────────
    125: {'theme': 'Leading Yourself', 'topic': 'Taking Initiative', 'target': 'mid_senior', 'reverse': False,
          'text_en': 'Is prepared to seize opportunities when they arise.',
          'text_th': 'เตรียมพร้อมและคว้าโอกาสที่ผ่านเข้ามาเสมอ'},
    126: {'theme': 'Leading Yourself', 'topic': 'Taking Initiative', 'target': 'mid_senior', 'reverse': False,
          'text_en': 'Would respond to a boss who provided autonomy by working hard to develop his/her skills.',
          'text_th': 'เมื่อหัวหน้าไว้ใจให้ทำงานเอง จะใช้โอกาสนั้นทำงานอย่างหนักและพัฒนาทักษะของตนเอง'},
    127: {'theme': 'Leading Yourself', 'topic': 'Taking Initiative', 'target': 'mid_senior', 'reverse': False,
          'text_en': 'Takes charge when trouble comes.',
          'text_th': 'กล้ารับผิดชอบและเข้าควบคุมสถานการณ์เมื่อเกิดปัญหา'},
    128: {'theme': 'Leading Yourself', 'topic': 'Taking Initiative', 'target': 'mid_senior', 'reverse': False,
          'text_en': 'Is creative or innovative.',
          'text_th': 'มีความคิดริเริ่มสร้างสรรค์และชอบนำเสนอนวัตกรรมใหม่ๆ'},
    129: {'theme': 'Leading Yourself', 'topic': 'Taking Initiative', 'target': 'mid_senior', 'reverse': False,
          'text_en': 'Can effectively lead an operation from its inception through completion.',
          'text_th': 'สามารถนำทีมดำเนินงานตั้งแต่เริ่มต้นจนสำเร็จได้อย่างมีประสิทธิภาพ'},

    # ── Leading Yourself: Executive Image (executive) ─────────────────────────
    130: {'theme': 'Leading Yourself', 'topic': 'Executive Image', 'target': 'executive', 'reverse': False,
          'text_en': 'Communicates confidence and steadiness during difficult times.',
          'text_th': 'สื่อสารด้วยความมั่นใจและหนักแน่น แม้ในช่วงเวลาที่ยากลำบาก'},
    131: {'theme': 'Leading Yourself', 'topic': 'Executive Image', 'target': 'executive', 'reverse': False,
          'text_en': 'Projects confidence and poise.',
          'text_th': 'แสดงออกถึงความมั่นใจและความสุขุม'},
    132: {'theme': 'Leading Yourself', 'topic': 'Executive Image', 'target': 'executive', 'reverse': False,
          'text_en': 'Adapts readily to new situations.',
          'text_th': 'พร้อมปรับตัวเข้ากับสถานการณ์ใหม่ๆ ได้อย่างรวดเร็ว'},
    133: {'theme': 'Leading Yourself', 'topic': 'Executive Image', 'target': 'executive', 'reverse': False,
          'text_en': 'Commands attention and respect.',
          'text_th': 'สามารถดึงดูดความสนใจและสร้างความเคารพนับถือจากคนรอบข้าง'},
    134: {'theme': 'Leading Yourself', 'topic': 'Executive Image', 'target': 'executive', 'reverse': False,
          'text_en': 'Accepts setbacks with grace.',
          'text_th': 'ยอมรับความล้มเหลวได้อย่างสง่างาม'},

    # ── Leading Yourself: Composure (mid_senior) ──────────────────────────────
    135: {'theme': 'Leading Yourself', 'topic': 'Composure', 'target': 'mid_senior', 'reverse': False,
          'text_en': 'Does not become hostile or moody when things are not going his/her way.',
          'text_th': 'ไม่แสดงท่าทีก้าวร้าวหรือหงุดหงิดเมื่อไม่ได้ดั่งใจ'},
    136: {'theme': 'Leading Yourself', 'topic': 'Composure', 'target': 'mid_senior', 'reverse': False,
          'text_en': 'Does not blame others or situations for his/her mistakes.',
          'text_th': 'ไม่โทษคนอื่นหรือสถานการณ์รอบข้างเมื่อตนเองทำพลาด'},
    137: {'theme': 'Leading Yourself', 'topic': 'Composure', 'target': 'mid_senior', 'reverse': False,
          'text_en': 'Contributes more to solving organizational problems than to complaining about them.',
          'text_th': 'เน้นการช่วยหาทางออกให้กับปัญหา มากกว่าการเอาแต่บ่น'},
    138: {'theme': 'Leading Yourself', 'topic': 'Composure', 'target': 'mid_senior', 'reverse': False,
          'text_en': 'Remains calm when crises occur.',
          'text_th': 'รักษาความสงบและมีสติได้ดีเมื่อเกิดวิกฤต'},

    # ── Leading Yourself: Balance Between Personal and Work Life (both) ───────
    139: {'theme': 'Leading Yourself', 'topic': 'Balance Between Personal and Work Life', 'target': 'both', 'reverse': False,
          'text_en': 'Acts as if there is more to life than just having a career.',
          'text_th': 'ใช้ชีวิตโดยตระหนักว่าชีวิตมีอะไรมากกว่าแค่เรื่องงาน'},
    140: {'theme': 'Leading Yourself', 'topic': 'Balance Between Personal and Work Life', 'target': 'both', 'reverse': False,
          'text_en': 'Has activities and interests outside of career.',
          'text_th': 'มีกิจกรรมและความสนใจอื่นๆ นอกเหนือจากงาน'},
    141: {'theme': 'Leading Yourself', 'topic': 'Balance Between Personal and Work Life', 'target': 'both', 'reverse': False,
          'text_en': 'Does not take career so seriously that his/her personal life suffers.',
          'text_th': 'ไม่จริงจังกับงานมากเกินไปจนทำให้ชีวิตส่วนตัวพัง'},

    # ── Leading Yourself: Learning from Experience (executive) ────────────────
    142: {'theme': 'Leading Yourself', 'topic': 'Learning from Experience', 'target': 'executive', 'reverse': False,
          'text_en': 'Reflects on and learns from experience.',
          'text_th': 'ทบทวนและเรียนรู้จากประสบการณ์'},
    143: {'theme': 'Leading Yourself', 'topic': 'Learning from Experience', 'target': 'executive', 'reverse': False,
          'text_en': 'Accepts responsibility for his or her problems.',
          'text_th': 'รับผิดชอบต่อปัญหาของตัวเอง'},
    144: {'theme': 'Leading Yourself', 'topic': 'Learning from Experience', 'target': 'executive', 'reverse': False,
          'text_en': 'Understands own weaknesses and how to compensate for them.',
          'text_th': 'เข้าใจจุดอ่อนของตนเองและรู้วิธีจัดการกับจุดอ่อนนั้น'},
    145: {'theme': 'Leading Yourself', 'topic': 'Learning from Experience', 'target': 'executive', 'reverse': False,
          'text_en': 'Seeks candid feedback on his or her performance.',
          'text_th': 'แสวงหาคำติชม (Feedback) ที่ตรงไปตรงมาเกี่ยวกับผลงานของตนเอง'},
    146: {'theme': 'Leading Yourself', 'topic': 'Learning from Experience', 'target': 'executive', 'reverse': False,
          'text_en': 'Changes behavior in response to feedback.',
          'text_th': 'ปรับเปลี่ยนพฤติกรรมตามคำแนะนำที่ได้รับ'},

    # ── Leading Yourself: Self-awareness (mid_senior) ─────────────────────────
    147: {'theme': 'Leading Yourself', 'topic': 'Self-awareness', 'target': 'mid_senior', 'reverse': False,
          'text_en': 'Admits personal mistakes, learns from them, and moves on to correct the situation.',
          'text_th': 'ยอมรับความผิดพลาด เรียนรู้จากมัน และลงมือแก้ไขสถานการณ์'},
    148: {'theme': 'Leading Yourself', 'topic': 'Self-awareness', 'target': 'mid_senior', 'reverse': False,
          'text_en': 'Does an honest self-assessment.',
          'text_th': 'ประเมินศักยภาพตัวเองตามความเป็นจริง'},
    149: {'theme': 'Leading Yourself', 'topic': 'Self-awareness', 'target': 'mid_senior', 'reverse': False,
          'text_en': 'Seeks corrective feedback to improve him/herself.',
          'text_th': 'แสวงหาคำแนะนำเพื่อนำมาปรับปรุงและพัฒนาตัวเองให้ดีขึ้น'},
    150: {'theme': 'Leading Yourself', 'topic': 'Self-awareness', 'target': 'mid_senior', 'reverse': False,
          'text_en': "Sorts out his/her strengths and weaknesses fairly accurately (i.e., knows him/herself).",
          'text_th': 'รู้จักจุดแข็งและจุดอ่อนของตนเองได้อย่างแม่นยำ'},

    # ── Leading Yourself: Credibility (executive) ─────────────────────────────
    151: {'theme': 'Leading Yourself', 'topic': 'Credibility', 'target': 'executive', 'reverse': False,
          'text_en': 'Uses ethical considerations to guide decisions.',
          'text_th': 'ยึดมั่นในจริยธรรมและความถูกต้องในการตัดสินใจ'},
    152: {'theme': 'Leading Yourself', 'topic': 'Credibility', 'target': 'executive', 'reverse': False,
          'text_en': 'Through words and deeds encourages honesty throughout the organization.',
          'text_th': 'ส่งเสริมความซื่อสัตย์ทั่วทั้งองค์กรผ่านคำพูดและการกระทำ'},
    153: {'theme': 'Leading Yourself', 'topic': 'Credibility', 'target': 'executive', 'reverse': False,
          'text_en': 'Speaks candidly about tough issues facing the organization.',
          'text_th': 'กล้าพูดถึงปัญหาที่ยากลำบากขององค์กรอย่างตรงไปตรงมา'},
    154: {'theme': 'Leading Yourself', 'topic': 'Credibility', 'target': 'executive', 'reverse': False,
          'text_en': 'Tells the truth, not just what important constituents want to hear.',
          'text_th': 'พูดความจริง ไม่ใช่เลือกพูดแต่สิ่งที่คนอยากฟัง'},
    155: {'theme': 'Leading Yourself', 'topic': 'Credibility', 'target': 'executive', 'reverse': False,
          'text_en': 'Can be trusted to maintain confidentiality.',
          'text_th': 'รักษาความลับได้ดีและเป็นคนที่ไว้วางใจได้'},
    156: {'theme': 'Leading Yourself', 'topic': 'Credibility', 'target': 'executive', 'reverse': False,
          'text_en': 'Places ethical behavior above personal gain.',
          'text_th': 'เห็นแก่ความถูกต้องทางจริยธรรมมากกว่าผลประโยชน์ส่วนตน'},
    157: {'theme': 'Leading Yourself', 'topic': 'Credibility', 'target': 'executive', 'reverse': False,
          'text_en': 'Follows through on promises.',
          'text_th': 'รักษาคำพูดและทำตามที่รับปากไว้'},
    158: {'theme': 'Leading Yourself', 'topic': 'Credibility', 'target': 'executive', 'reverse': False,
          'text_en': 'Acts in accordance with his or her stated values.',
          'text_th': 'ปฏิบัติตนตามค่านิยมที่ตนเองยึดถือ'},

    # ── Leading Yourself: Career Management (mid_senior) ─────────────────────
    159: {'theme': 'Leading Yourself', 'topic': 'Career Management', 'target': 'mid_senior', 'reverse': False,
          'text_en': 'Understands the value of a good mentoring relationship.',
          'text_th': 'เข้าใจถึงคุณค่าของการมีความสัมพันธ์แบบพี่เลี้ยง (Mentor) ที่ดี'},
    160: {'theme': 'Leading Yourself', 'topic': 'Career Management', 'target': 'mid_senior', 'reverse': False,
          'text_en': 'Effectively builds and maintains feedback channels.',
          'text_th': 'สร้างและรักษาช่องทางการสื่อสารเพื่อรับฟัง Feedback อย่างมีประสิทธิภาพ'},
    161: {'theme': 'Leading Yourself', 'topic': 'Career Management', 'target': 'mid_senior', 'reverse': False,
          'text_en': 'Responds to feedback from direct reports.',
          'text_th': 'ตอบสนองต่อคำติชมจากลูกน้องได้ดี'},
    162: {'theme': 'Leading Yourself', 'topic': 'Career Management', 'target': 'mid_senior', 'reverse': False,
          'text_en': 'Actively cultivates a good relationship with superior.',
          'text_th': 'สร้างความสัมพันธ์ที่ดีกับหัวหน้างานอย่างสม่ำเสมอ'},
    163: {'theme': 'Leading Yourself', 'topic': 'Career Management', 'target': 'mid_senior', 'reverse': False,
          'text_en': 'Uses mentoring relationships effectively.',
          'text_th': 'ใช้ประโยชน์จากการปรึกษาพี่เลี้ยงได้อย่างเต็มที่และมีประสิทธิภาพ'},
    164: {'theme': 'Leading Yourself', 'topic': 'Career Management', 'target': 'mid_senior', 'reverse': False,
          'text_en': 'Actively seeks opportunities to develop professional relationships with others.',
          'text_th': 'ขวนขวายหาโอกาสในการสร้างเครือข่ายความสัมพันธ์ทางวิชาชีพกับผู้อื่น'},
    165: {'theme': 'Leading Yourself', 'topic': 'Career Management', 'target': 'mid_senior', 'reverse': False,
          'text_en': 'Responds effectively to constructive criticism from others.',
          'text_th': 'รับฟังและจัดการกับคำติชมได้อย่างดี'},

    # ── Team Effectiveness: Psychological Safety (peer_lower) ────────────────
    # Note: items 166, 168, 170 are negatively worded → reverse coded
    166: {'theme': 'Team Effectiveness', 'topic': 'Psychological Safety', 'target': 'peer_lower', 'reverse': True,
          'text_en': 'If I make a mistake on this team, it is often held against me.',
          'text_th': 'ถ้าฉันทำผิดพลาด ฉันมักถูกตำหนิ'},
    167: {'theme': 'Team Effectiveness', 'topic': 'Psychological Safety', 'target': 'peer_lower', 'reverse': False,
          'text_en': 'Members of this team are able to bring up problems and tough issues.',
          'text_th': 'สมาชิกในทีมพูดคุยกันเรื่องปัญหาหรือประเด็นหนักหน่วงได้'},
    168: {'theme': 'Team Effectiveness', 'topic': 'Psychological Safety', 'target': 'peer_lower', 'reverse': True,
          'text_en': 'People on this team sometimes reject others for being different.',
          'text_th': 'บางครั้งคนในทีมก็ปฏิเสธคนคิดต่าง'},
    169: {'theme': 'Team Effectiveness', 'topic': 'Psychological Safety', 'target': 'peer_lower', 'reverse': False,
          'text_en': 'It is safe to take a risk on this team.',
          'text_th': 'การกล้าเสี่ยงนั้นเป็นเรื่องที่ปลอดภัยในทีมนี้'},
    170: {'theme': 'Team Effectiveness', 'topic': 'Psychological Safety', 'target': 'peer_lower', 'reverse': True,
          'text_en': 'It is difficult to ask other members of this team for help.',
          'text_th': 'การขอความช่วยเหลือจากสมาชิกในทีมเป็นเรื่องยาก'},
    171: {'theme': 'Team Effectiveness', 'topic': 'Psychological Safety', 'target': 'peer_lower', 'reverse': False,
          'text_en': 'No one on this team would deliberately act in a way that undermines my efforts.',
          'text_th': 'ไม่มีใครในทีมจงใจทำสิ่งที่ขัดขวางความพยายามของคุณ'},
    172: {'theme': 'Team Effectiveness', 'topic': 'Psychological Safety', 'target': 'peer_lower', 'reverse': False,
          'text_en': 'Working with members of this team, my unique skills and talents are valued and utilized.',
          'text_th': 'คุณได้นำทักษะและพรสวรรค์เฉพาะตัวมาใช้และคนในทีมเห็นคุณค่า'},

    # ── Open-Ended (text response) ────────────────────────────────────────────
    173: {'theme': 'Open-Ended Feedback', 'topic': 'Open-Ended Feedback', 'target': 'both', 'reverse': False,
          'text_en': "What are {name}'s most significant strengths as a leader?",
          'text_th': 'จุดแข็งที่โดดเด่นที่สุดของ {name} ในฐานะผู้นำคืออะไร?'},
    174: {'theme': 'Open-Ended Feedback', 'topic': 'Open-Ended Feedback', 'target': 'both', 'reverse': False,
          'text_en': "What are {name}'s most significant development needs as a leader?",
          'text_th': 'สิ่งที่ {name} ควรพัฒนามากที่สุดในฐานะผู้นำคืออะไร?'},
    175: {'theme': 'Open-Ended Feedback', 'topic': 'Open-Ended Feedback', 'target': 'executive', 'reverse': False,
          'text_en': 'In what ways could {name} do more to develop other leaders in the organization?',
          'text_th': '{name} สามารถทำอะไรเพิ่มเติมได้อีกบ้าง เพื่อช่วยพัฒนาผู้นำคนอื่นๆ ในองค์กร?'},
    176: {'theme': 'Open-Ended Feedback', 'topic': 'Open-Ended Feedback', 'target': 'executive', 'reverse': False,
          'text_en': 'What is one thing that {name} could do to increase their impact as a leader?',
          'text_th': 'สิ่งหนึ่งที่ {name} ทำแล้วจะช่วยเพิ่มผลกระทบเชิงบวก (Impact) ในฐานะผู้นำได้มากที่สุดคืออะไร?'},

    # ── Team Effectiveness: Dependability (peer_lower) ────────────────────────
    177: {'theme': 'Team Effectiveness', 'topic': 'Dependability', 'target': 'peer_lower', 'reverse': False,
          'text_en': 'When team members say they will do something, they follow through.',
          'text_th': 'เมื่อเพื่อนร่วมทีมบอกว่าจะทำอะไร พวกเขาทำจริงตามที่พูดไว้'},
    178: {'theme': 'Team Effectiveness', 'topic': 'Dependability', 'target': 'peer_lower', 'reverse': False,
          'text_en': 'Team members consistently deliver quality work on time.',
          'text_th': 'สมาชิกในทีมส่งมอบงานที่มีคุณภาพตรงเวลาอย่างสม่ำเสมอ'},

    # ── Team Effectiveness: Clarity (peer_lower) ──────────────────────────────
    179: {'theme': 'Team Effectiveness', 'topic': 'Clarity', 'target': 'peer_lower', 'reverse': False,
          'text_en': 'I clearly understand my role, duties, and responsibilities.',
          'text_th': 'ฉันเข้าใจบทบาท หน้าที่ ความรับผิดชอบ ของตัวเองอย่างชัดเจน'},
    180: {'theme': 'Team Effectiveness', 'topic': 'Clarity', 'target': 'peer_lower', 'reverse': False,
          'text_en': "I know what the team's goals are and how to achieve them.",
          'text_th': 'ฉันรู้ว่าทีมมีเป้าหมายอะไร และรู้ว่าต้องทำอย่างไรถึงจะไปถึงเป้า'},
    181: {'theme': 'Team Effectiveness', 'topic': 'Clarity', 'target': 'peer_lower', 'reverse': False,
          'text_en': 'The team has a clear and effective decision-making process.',
          'text_th': 'ทีมมีกระบวนการตัดสินใจที่ชัดเจนและมีประสิทธิภาพ'},

    # ── Team Effectiveness: Meaning (peer_lower) ──────────────────────────────
    182: {'theme': 'Team Effectiveness', 'topic': 'Meaning', 'target': 'peer_lower', 'reverse': False,
          'text_en': 'The work I do is fulfilling both personally and professionally.',
          'text_th': 'งานที่ทำอยู่ช่วยเติมเต็มความรู้สึก ทั้งในมุมของชีวิตส่วนตัวและความก้าวหน้าในอาชีพ'},
    183: {'theme': 'Team Effectiveness', 'topic': 'Meaning', 'target': 'peer_lower', 'reverse': False,
          'text_en': 'The work assigned to me aligns with my strengths and interests.',
          'text_th': 'งานที่ได้รับมอบหมายสอดคล้องกับความถนัดและความสนใจของฉัน'},
    184: {'theme': 'Team Effectiveness', 'topic': 'Meaning', 'target': 'peer_lower', 'reverse': False,
          'text_en': 'When I accomplish something or make progress, I receive recognition from the team.',
          'text_th': 'เมื่อทำงานสำเร็จหรือมีความคืบหน้า ฉันมักได้รับคำชมหรือการยอมรับจากทีม'},

    # ── Team Effectiveness: Impact (peer_lower) ───────────────────────────────
    185: {'theme': 'Team Effectiveness', 'topic': 'Impact', 'target': 'peer_lower', 'reverse': False,
          'text_en': "I understand how the team's work contributes to the organization's goals.",
          'text_th': 'ฉันเข้าใจว่างานของทีมช่วยให้องค์กรบรรลุเป้าหมายได้อย่างไร'},
    186: {'theme': 'Team Effectiveness', 'topic': 'Impact', 'target': 'peer_lower', 'reverse': False,
          'text_en': 'I can clearly see that the work I do is making a positive difference.',
          'text_th': 'ฉันมองเห็นได้ชัดว่างานที่ทำอยู่กำลังสร้างการเปลี่ยนแปลงในทางที่ดีขึ้น'},
    187: {'theme': 'Team Effectiveness', 'topic': 'Impact', 'target': 'peer_lower', 'reverse': False,
          'text_en': "The team's current way of working supports good mental health and prevents burnout.",
          'text_th': 'รูปแบบการทำงานของทีมปัจจุบัน เอื้อต่อสุขภาพใจที่ดีและไม่ทำให้รู้สึกหมดไฟ'},

    # ── Leading Organization: Sound Judgment (executive) ─────────────────────
    188: {'theme': 'Leading Organization', 'topic': 'Sound Judgment', 'target': 'executive', 'reverse': False,
          'text_en': 'Sees underlying concepts and patterns in complex situations.',
          'text_th': 'อ่านสถานการณ์ที่ซับซ้อนออก สามารถมองทะลุถึงแก่นและความเชื่อมโยงของปัญหา'},
    189: {'theme': 'Leading Organization', 'topic': 'Sound Judgment', 'target': 'executive', 'reverse': False,
          'text_en': 'Gives appropriate weight to the concerns of key stakeholders.',
          'text_th': 'รับฟังและให้ความสำคัญกับความกังวลของผู้มีส่วนได้ส่วนเสีย และคนที่เกี่ยวข้องอย่างเหมาะสม'},
    190: {'theme': 'Leading Organization', 'topic': 'Sound Judgment', 'target': 'executive', 'reverse': False,
          'text_en': 'Readily grasps the crux of an issue despite having ambiguous information.',
          'text_th': 'เข้าใจแก่นของปัญหาได้รวดเร็ว แม้สถานการณ์ยังคลุมเครือ'},
    191: {'theme': 'Leading Organization', 'topic': 'Sound Judgment', 'target': 'executive', 'reverse': False,
          'text_en': 'Makes effective decisions in a timely manner.',
          'text_th': 'ตัดสินใจได้อย่างมีประสิทธิภาพและทันต่อสถานการณ์'},
    192: {'theme': 'Leading Organization', 'topic': 'Sound Judgment', 'target': 'executive', 'reverse': False,
          'text_en': 'Accurately differentiates between important and unimportant issues.',
          'text_th': 'รู้ชัดว่าเรื่องไหนควรให้ความสำคัญและเรื่องไหนวางไว้ก่อนได้'},
    193: {'theme': 'Leading Organization', 'topic': 'Sound Judgment', 'target': 'executive', 'reverse': False,
          'text_en': 'Develops solutions that effectively address underlying problems.',
          'text_th': 'คิดหาแนวทางแก้ปัญหาที่ตอบโจทย์ต้นตอของปัญหาได้จริง'},

    # ── Leading Others: Inspiring Commitment (executive) ─────────────────────
    194: {'theme': 'Leading Others', 'topic': 'Inspiring Commitment', 'target': 'executive', 'reverse': False,
          'text_en': 'Rallies support throughout the organization to get things done.',
          'text_th': 'ระดมความร่วมมือจากทุกส่วนขององค์กรเพื่อขับเคลื่อนงานให้สำเร็จ'},
    195: {'theme': 'Leading Others', 'topic': 'Inspiring Commitment', 'target': 'executive', 'reverse': False,
          'text_en': 'Publicly praises others for their performance.',
          'text_th': 'ยกย่องชมเชยคนที่ทำผลงานได้ดี ต่อหน้าธารกำนัล'},
    196: {'theme': 'Leading Others', 'topic': 'Inspiring Commitment', 'target': 'executive', 'reverse': False,
          'text_en': 'Infuses the organization with a sense of purpose.',
          'text_th': 'ทำให้คนรู้สึกว่าองค์กรมีทิศทางและเป้าหมายที่ชัดเจน'},
    197: {'theme': 'Leading Others', 'topic': 'Inspiring Commitment', 'target': 'executive', 'reverse': False,
          'text_en': 'Understands what motivates other people to perform at their best.',
          'text_th': 'รู้ว่าอะไรทำให้แต่ละคนอยากทุ่มเททำงานให้ดีที่สุด'},
    198: {'theme': 'Leading Others', 'topic': 'Inspiring Commitment', 'target': 'executive', 'reverse': False,
          'text_en': 'Provides tangible rewards for significant organizational achievements.',
          'text_th': 'ให้รางวัลกับคนที่ทำผลงานสำคัญให้กับองค์กรได้สำเร็จ'},
}

# ─────────────────────────────────────────────
# 2. Question ID sets by section
# ─────────────────────────────────────────────

OPEN_ENDED_QIDS = {173, 174, 175, 176}

TEAM_EFFECTIVENESS_QIDS = {166, 167, 168, 169, 170, 171, 172, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187}

# Team Effectiveness dimension groupings (correct Pete repo IDs)
TEAM_GROUPS = {
    'Psychological Safety': [166, 167, 168, 169, 170, 171, 172],
    'Dependability': [177, 178],
    'Clarity': [179, 180, 181],
    'Meaning': [182, 183, 184],
    'Impact': [185, 186, 187],
}

# Open-ended labels
OPEN_ENDED_LABELS = {
    173: {'en': 'Strengths',         'th': 'จุดแข็งที่โดดเด่นที่สุดของบุคคลนี้ในฐานะผู้นำคืออะไร?'},
    174: {'en': 'Development Needs', 'th': 'สิ่งที่บุคคลนี้ควรพัฒนามากที่สุดในฐานะผู้นำคืออะไร?'},
    175: {'en': 'Develop Leaders',   'th': 'บุคคลนี้สามารถทำอะไรเพิ่มเติมได้อีกบ้าง เพื่อช่วยพัฒนาผู้นำคนอื่นๆ ในองค์กร?'},
    176: {'en': 'Increase Impact',   'th': 'สิ่งหนึ่งที่บุคคลนี้ทำแล้วจะช่วยเพิ่มผลกระทบเชิงบวก (Impact) ในฐานะผู้นำได้มากที่สุดคืออะไร?'},
}


def get_ccl_themes(student_level):
    """Return CCL theme/topic/qid structure appropriate for the student's level.
    Includes:
      - topics with target == student_level
      - topics with target == 'both'
    """
    topics_for_level = {}
    for qid, q in QUESTION_DATA.items():
        if q['theme'] in ('Team Effectiveness', 'Open-Ended Feedback'):
            continue
        target = q['target']
        if target == student_level or target == 'both':
            theme = q['theme']
            topic = q['topic']
            if theme not in topics_for_level:
                topics_for_level[theme] = {}
            if topic not in topics_for_level[theme]:
                topics_for_level[theme][topic] = []
            topics_for_level[theme][topic].append(qid)

    # Sort qids within each topic
    for theme in topics_for_level:
        for topic in topics_for_level[theme]:
            topics_for_level[theme][topic].sort()

    return topics_for_level


# ─────────────────────────────────────────────
# 3. Data fetching: Supabase or CSV fallback
# ─────────────────────────────────────────────

def _is_supabase_configured():
    return (
        SUPABASE_URL != 'https://YOUR_PROJECT.supabase.co'
        and SUPABASE_ANON_KEY != 'YOUR_ANON_KEY'
        and SUPABASE_URL.strip()
        and SUPABASE_ANON_KEY.strip()
    )


def fetch_from_supabase():
    """Fetch session data from Supabase using nested REST API.
    Returns a flat list of row dicts matching the CSV column schema.
    """
    import requests as _requests

    select = (
        'name,nickname,email,level,role_name,company,'
        'evaluations('
          'id,evaluator_email,evaluator_level,is_self_evaluation,is_completed,'
          'started_at,completed_at,'
          'responses(question_id,score,text_response,is_skipped,metadata)'
        ')'
    )
    url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/students"
    headers = {
        'apikey': SUPABASE_ANON_KEY,
        'Authorization': f'Bearer {SUPABASE_ANON_KEY}',
        'Accept': 'application/json',
    }
    params = {
        'select': select,
        'session_id': f'eq.{SESSION_ID}',
    }
    resp = _requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    students = resp.json()

    # Flatten nested structure into row dicts matching CSV schema
    rows = []
    for st in students:
        for ev in (st.get('evaluations') or []):
            for r in (ev.get('responses') or []):
                rows.append({
                    'student_name':           st.get('name', ''),
                    'student_nickname':       st.get('nickname', '') or '',
                    'student_email':          st.get('email', '') or '',
                    'student_level':          st.get('level', '') or '',
                    'student_role_name':      st.get('role_name', '') or '',
                    'student_company':        st.get('company', '') or '',
                    'evaluation_id':          ev.get('id', ''),
                    'evaluator_email':        ev.get('evaluator_email', '') or '',
                    'evaluator_level':        ev.get('evaluator_level', '') or '',
                    'is_self_evaluation':     ev.get('is_self_evaluation', False),
                    'is_completed':           ev.get('is_completed', False),
                    'evaluation_started_at':  ev.get('started_at', ''),
                    'evaluation_completed_at':ev.get('completed_at', '') or '',
                    'question_id':            str(r.get('question_id', '')),
                    'score':                  r.get('score'),
                    'text_response':          r.get('text_response', '') or '',
                    'is_skipped':             r.get('is_skipped', False),
                    'metadata':               r.get('metadata'),
                })
    return rows


def load_rows_from_csv():
    """Load raw rows from the CSV export."""
    rows = []
    with open(FEEDBACK_CSV, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def normalise_bool(val):
    """Normalise boolean values from both Supabase (Python bool) and CSV ('true'/'false' string)."""
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.lower() == 'true'
    return bool(val)


# ─────────────────────────────────────────────
# 4. Score helpers
# ─────────────────────────────────────────────

def safe_score(val):
    if val is None or val == 'null' or val == '':
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def apply_reverse(score, reverse):
    if score is None:
        return None
    return (6.0 - score) if reverse else score


def avg(scores):
    valid = [s for s in scores if s is not None]
    if not valid:
        return None
    return round(sum(valid) / len(valid), 3)


# ─────────────────────────────────────────────
# 5. Main processing
# ─────────────────────────────────────────────

def process_data():
    # ── Load data ──
    if _is_supabase_configured():
        print("Fetching data from Supabase...")
        all_rows = fetch_from_supabase()
        print(f"  Fetched {len(all_rows)} response rows")
    else:
        print(f"Supabase not configured — reading from CSV: {FEEDBACK_CSV}")
        all_rows = load_rows_from_csv()
        print(f"  Read {len(all_rows)} rows")

    # ── Group rows by student ──
    student_rows = defaultdict(list)
    student_info = {}
    for row in all_rows:
        name = row['student_name']
        student_rows[name].append(row)
        if name not in student_info:
            student_info[name] = {
                'name':     name,
                'nickname': row.get('student_nickname', ''),
                'email':    row.get('student_email', ''),
                'level':    row.get('student_level', ''),
                'role':     row.get('student_role_name', ''),
                'company':  row.get('student_company', ''),
            }

    students_output = []

    for name in sorted(student_rows.keys()):
        rows = student_rows[name]
        info = student_info[name]

        student_level = info['level']  # 'executive' or 'mid_senior'
        ccl_themes = get_ccl_themes(student_level)

        # ── Evaluator counts ──
        eval_ids = {'higher': set(), 'equal': set(), 'lower': set(), 'self': set()}
        for r in rows:
            ev_id = r['evaluation_id']
            if normalise_bool(r['is_self_evaluation']):
                eval_ids['self'].add(ev_id)
            else:
                level = r['evaluator_level']
                if level in eval_ids:
                    eval_ids[level].add(ev_id)
        evaluator_counts = {k: len(v) for k, v in eval_ids.items()}

        # ── Per-question scores ──
        q_scores = defaultdict(lambda: {
            'others': [], 'self': None,
            'higher': [], 'equal': [], 'lower': [],
        })

        for r in rows:
            if normalise_bool(r.get('is_skipped', False)):
                continue
            qid_str = str(r['question_id'])
            if not qid_str.isdigit():
                continue
            qid = int(qid_str)
            if qid not in QUESTION_DATA:
                continue  # unknown question ID — skip

            raw = safe_score(r['score'])
            if raw is None:
                continue

            q_meta = QUESTION_DATA[qid]
            score = apply_reverse(raw, q_meta['reverse'])
            is_self = normalise_bool(r['is_self_evaluation'])
            ev_level = r['evaluator_level']

            if is_self:
                q_scores[qid]['self'] = score
            else:
                q_scores[qid]['others'].append(score)
                if ev_level in ('higher', 'equal', 'lower'):
                    q_scores[qid][ev_level].append(score)

        # ── CCL Competency topic averages ──
        def build_ccl_data(themes_dict):
            result = {}
            for theme, topics in themes_dict.items():
                result[theme] = {}
                for topic, qids in topics.items():
                    others_scores, self_scores = [], []
                    higher_scores, equal_scores, lower_scores = [], [], []
                    questions = []
                    for qid in qids:
                        if qid not in q_scores:
                            continue
                        qs = q_scores[qid]
                        q_meta = QUESTION_DATA.get(qid, {})
                        q_others_avg = avg(qs['others'])
                        q_self = qs['self']
                        others_scores.extend(qs['others'])
                        if q_self is not None:
                            self_scores.append(q_self)
                        higher_scores.extend(qs['higher'])
                        equal_scores.extend(qs['equal'])
                        lower_scores.extend(qs['lower'])
                        questions.append({
                            'qid':         qid,
                            'question_en': q_meta.get('text_en', ''),
                            'question_th': q_meta.get('text_th', ''),
                            'others_avg':  q_others_avg,
                            'self':        q_self,
                            'higher_avg':  avg(qs['higher']),
                            'equal_avg':   avg(qs['equal']),
                            'lower_avg':   avg(qs['lower']),
                            'n_others':    len(qs['others']),
                        })
                    result[theme][topic] = {
                        'others_avg': avg(others_scores),
                        'self_avg':   avg(self_scores),
                        'higher_avg': avg(higher_scores),
                        'equal_avg':  avg(equal_scores),
                        'lower_avg':  avg(lower_scores),
                        'questions':  questions,
                    }
            return result

        ccl_data = build_ccl_data(ccl_themes)

        # ── Theme averages ──
        theme_avgs = {}
        for theme, topics in ccl_themes.items():
            all_others, all_self = [], []
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
                'self_avg':   avg(all_self),
            }

        # ── Top/Bottom topics + Johari ──
        topic_list = []
        for theme, topics in ccl_themes.items():
            for topic_name, qids in topics.items():
                td = ccl_data[theme][topic_name]
                gap = None
                if td['self_avg'] is not None and td['others_avg'] is not None:
                    gap = round(td['self_avg'] - td['others_avg'], 3)
                topic_list.append({
                    'theme':      theme,
                    'topic':      topic_name,
                    'others_avg': td['others_avg'],
                    'self_avg':   td['self_avg'],
                    'gap':        gap,
                })

        valid_topics = [t for t in topic_list if t['others_avg'] is not None]
        sorted_by_others = sorted(valid_topics, key=lambda x: x['others_avg'], reverse=True)
        top_strengths = sorted_by_others[:5]
        dev_areas     = sorted_by_others[-5:][::-1]

        topics_with_gap = [t for t in valid_topics if t['gap'] is not None]
        blind_spots      = sorted(topics_with_gap, key=lambda x: x['gap'])[:5]
        hidden_strengths = sorted(topics_with_gap, key=lambda x: x['gap'], reverse=True)[:5]

        # ── Team Effectiveness (Edmondson / Google) ──
        team_effectiveness = []
        for dim_name, qids in TEAM_GROUPS.items():
            dim_others, dim_higher, dim_equal, dim_lower, dim_self = [], [], [], [], []
            dim_questions = []
            for qid in qids:
                q_meta = QUESTION_DATA.get(qid, {})
                qs = q_scores.get(qid, {'others': [], 'self': None, 'higher': [], 'equal': [], 'lower': []})
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
                    'qid':        qid,
                    'question_en': q_meta.get('text_en', ''),
                    'question_th': q_meta.get('text_th', ''),
                    'others_avg': avg(qs['others']),
                    'higher_avg': avg(qs['higher']),
                    'equal_avg':  avg(qs['equal']),
                    'lower_avg':  avg(qs['lower']),
                    'self':       qs['self'],
                    'n_others':   len(qs['others']),
                    'reverse':    q_meta.get('reverse', False),
                })
            team_effectiveness.append({
                'dimension':  dim_name,
                'others_avg': avg(dim_others),
                'higher_avg': avg(dim_higher),
                'equal_avg':  avg(dim_equal),
                'lower_avg':  avg(dim_lower),
                'self_avg':   avg(dim_self),
                'questions':  dim_questions,
            })

        # ── Open-ended responses ──
        open_ended = {}
        for r in rows:
            qid_str = str(r['question_id'])
            if not qid_str.isdigit():
                continue
            qid = int(qid_str)
            if qid not in OPEN_ENDED_QIDS:
                continue
            if normalise_bool(r.get('is_skipped', False)):
                continue
            text = r.get('text_response', '') or ''
            if not text.strip() or text.strip() == '/':
                continue

            is_self = normalise_bool(r['is_self_evaluation'])
            level = 'self' if is_self else r['evaluator_level']

            if qid not in open_ended:
                open_ended[qid] = {
                    'qid':      qid,
                    'label_en': OPEN_ENDED_LABELS.get(qid, {}).get('en', f'Q{qid}'),
                    'label_th': OPEN_ENDED_LABELS.get(qid, {}).get('th', ''),
                    'responses': {'higher': [], 'equal': [], 'lower': [], 'self': []},
                }
            if level in open_ended[qid]['responses']:
                open_ended[qid]['responses'][level].append(text.strip())

        open_ended_list = [open_ended[qid] for qid in sorted(open_ended.keys())]

        # ── Assemble student output ──
        students_output.append({
            'name':              info['name'],
            'nickname':          info['nickname'],
            'email':             info['email'],
            'level':             student_level,
            'role':              info['role'],
            'company':           info['company'],
            'evaluator_counts':  evaluator_counts,
            'theme_averages':    theme_avgs,
            'ccl_competencies':  ccl_data,
            'top_strengths':     top_strengths,
            'development_areas': dev_areas,
            'blind_spots':       blind_spots,
            'hidden_strengths':  hidden_strengths,
            'team_effectiveness': team_effectiveness,
            'open_ended':        open_ended_list,
        })

    output = {
        'meta': {
            'title':           'CCL 360-Degree Leadership Assessment',
            'subtitle':        'Executive Cohort Report',
            'generated':       '2026-03-19',
            'total_students':  len(students_output),
            'session_id':      SESSION_ID,
            'data_source':     'supabase' if _is_supabase_configured() else 'csv',
        },
        'students': students_output,
    }

    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\nWrote {OUTPUT_JSON}")
    print(f"Students processed: {len(students_output)}")
    for s in students_output:
        ec = s['evaluator_counts']
        print(f"  {s['name']} ({s['nickname']}) [{s['level']}]: "
              f"higher={ec['higher']}, equal={ec['equal']}, lower={ec['lower']}, self={ec['self']}")


if __name__ == '__main__':
    process_data()
