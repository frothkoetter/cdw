INSERT INTO customer_complaints 
(complaint_date, flightnum, uniquecarrier, origin_airport, customer_email, complaint_category, complaint_text, severity_score, is_resolved, resolution_notes)
VALUES
('2026-01-15 08:30:00', '1242', 'AA', 'ORD', 'j.smith@email.com', 'Delay', 'Flight was delayed 4 hours with no updates at the gate.', 4, TRUE, 'Issued $50 travel voucher.'),
('2026-01-16 10:15:00', '0451', 'DL', 'ATL', 'm.jones@email.com', 'Lost Luggage', 'Checked bag never arrived in Seattle. Contains medical supplies.', 5, FALSE, 'Currently tracing bag via S3 sensor data.'),
('2026-01-18 14:20:00', '3301', 'WN', 'DAL', 'flyer99@email.com', 'Staff', 'Gate agent was extremely rude when I asked about seat changes.', 2, TRUE, 'Staff member coached on customer service standards.'),
('2026-02-01 09:00:00', '0088', 'UA', 'SFO', 'tech_guru@email.com', 'In-Flight Service', 'The Wi-Fi I paid for did not work for the entire 6-hour flight.', 3, FALSE, NULL),
('2026-02-05 22:10:00', '2112', 'B6', 'JFK', 'b.wilson@email.com', 'Cancellation', 'Flight cancelled last minute; had to book a hotel out of pocket.', 5, TRUE, 'Reimbursed hotel stay and provided 5,000 miles.');
