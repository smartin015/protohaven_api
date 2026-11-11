import { extraFromName, fetch_events, LEVELS, process } from './lib';

describe('extraFromName', () => {
	test('parses area, level, title, and parenthetical suffix', () => {
		expect(extraFromName('Graphics 111: Printed Mugs (Dye Sublimation Clearance)')).toEqual({
			area: 'Graphics',
			level: 111,
			levelDesc: 'Beginner Project',
			title: 'Printed Mugs',
		});
	});

	test('parses names without a numeric level', () => {
		expect(extraFromName('Graphics: Printed Mugs')).toEqual({
			area: 'Graphics',
			level: null,
			levelDesc: '',
			title: 'Printed Mugs',
		});
	});

	test('marks uncategorized names as special events', () => {
		expect(extraFromName('Printed Mugs')).toEqual({
			area: 'Special Event',
			level: null,
			title: 'Printed Mugs',
		});
	});
});

describe('LEVELS', () => {
	test('maps levels from highest to lowest', () => {
		expect(LEVELS).toEqual([
			[200, 'Intermediate'],
			[110, 'Beginner Project'],
			[100, 'Beginner Skills'],
		]);
	});
});

describe('process', () => {
	beforeAll(() => {
		// jsdom does not implement innerText, which parseDesc relies on in browsers.
		Object.defineProperty(global.HTMLElement.prototype, 'innerText', {
			get() {
				return this.textContent;
			},
			configurable: true,
		});
	});

	function runProcess(description) {
		const events = [
			{
				id: '1',
				name: 'Wood 101',
				description,
				image_url: null,
				humanized_session_info: 'Single 3h Class',
				humanized_start: 'Thu, Jan 01, 10:00AM',
				start: '2026-01-01T10:00:00-05:00',
				end: '2026-01-01T13:00:00-05:00',
				capacity: 6,
				url: 'https://www.eventbrite.com/e/1/',
			},
		];
		const classes = {};
		process(events, classes, new Set(), new Set());
		return classes;
	}

	test('leaves age unknown when Airtable signal is absent', () => {
		const classes = runProcess('<p>Summary only</p>');
		expect(classes['Wood 101'].age).toBeNull();
	});

	test('sets age from the Airtable age requirement signal', () => {
		const classes = runProcess(
			'<p><strong>Age Requirement</strong></p><p>12+</p>'
		);
		expect(classes['Wood 101'].age).toBe(12);
	});
});

describe('fetch_events', () => {
	const originalFetch = global.fetch;

	afterEach(() => {
		global.fetch = originalFetch;
	});

	test('returns event data from the first successful fetch', async () => {
		const events = [{ name: 'Wood 101' }];
		global.fetch = jest.fn().mockResolvedValue({
			ok: true,
			json: async () => ({ events }),
		});

		await expect(fetch_events(3, 1000)).resolves.toEqual({ events });
	});
});
