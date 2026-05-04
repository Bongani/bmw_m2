import { Producer } from 'k6/x/kafka';
import { b64encode } from 'k6/encoding';

const brokers = [__ENV.KAFKA_BOOTSTRAP_SERVERS || 'kafka:29092'];

const clickProducer = new Producer({
    brokers: brokers,
    topic: 'ad_clicks',
});

const pageViewProducer = new Producer({
    brokers: brokers,
    topic: 'page_views',
});

export const options = {
    stages: [
        { duration: '30s', target: 5  },  // ramp up to 5 VUs
        { duration: '2m',  target: 5  },  // steady state
        { duration: '30s', target: 20 },  // spike
        { duration: '1m',  target: 20 },  // sustain spike
        { duration: '20s', target: 0  },  // ramp down
    ],
    thresholds: {
        kafka_writer_error_count: ['count == 0'],
    },
};

const CAMPAIGNS = ['campaign_A', 'campaign_B', 'campaign_C', 'campaign_D', 'campaign_E'];
const USER_COUNT = 50;

function toEventTime(ms) {
    return new Date(ms).toISOString().replace(/\.\d{3}Z$/, '').replace('Z', '');
}

export default function () {
    const userId = `user_${Math.floor(Math.random() * USER_COUNT)}`;

    // Event time: random offset within the last 10 minutes
    const now = new Date();
    const eventTimeMs = now.getTime() - Math.floor(Math.random() * 10 * 60 * 1000);
    const eventTime = toEventTime(eventTimeMs);

    // Produce 1 click
    const click = {
        user_id: userId,
        event_time: eventTime,
        campaign_id: CAMPAIGNS[Math.floor(Math.random() * CAMPAIGNS.length)],
        click_id: `click_${__VU}_${__ITER}`,
    };

    clickProducer.produce({
        messages: [{
            key: b64encode(userId),
            value: b64encode(JSON.stringify(click)),
        }],
    });

    // Produce 2-3 page views per click for a realistic attribution ratio
    const pageViewCount = Math.random() < 0.5 ? 2 : 3;
    const pvMessages = [];

    for (let i = 0; i < pageViewCount; i++) {
        const pvOffsetMs = (1 + Math.floor(Math.random() * 14)) * 60 * 1000;
        const pvTime = toEventTime(eventTimeMs + pvOffsetMs);

        const pageView = {
            user_id: userId,
            event_time: pvTime,
            url: `https://example.com/product_${Math.floor(Math.random() * 20)}`,
            event_id: `pv_${__VU}_${__ITER}_${i}`,
        };

        pvMessages.push({
            key: b64encode(userId),
            value: b64encode(JSON.stringify(pageView)),
        });
    }

    pageViewProducer.produce({ messages: pvMessages });
}

export function teardown() {
    clickProducer.close();
    pageViewProducer.close();
}
