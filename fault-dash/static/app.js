// from https://developer.mozilla.org/en-US/docs/Web/API/Fetch_API/Using_Fetch
async function postData(url, data) {
    const response = await fetch(url, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(data)
    }).catch(e => console.error(e));
    return response.json();
}

async function uploadFile(url, name, file, filename) {
    let data = {'name': name, 'file': file, 'filename': filename};
    const response = await fetch(url, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(data)
    }).catch(e => console.error(e));
    return response;
}

window.addEventListener("load", function() {

    Vue.component('statustable', {
        props: ['statuses'],
        data: function() {
            return {
                sortBy: "last_detected",
                sortDesc: false,
                codeModal: {
                    id: 'code-modal',
                    content: ''
                },
                fields: ["name", "last_detected", "since", "message", "details", "implementation"]
            }
        },
        computed: {
            items: function() {
                return this.statuses.map((val, idx) => {
                    var {pkey, ...val} = val;
                    val['primary-key'] = pkey;
                    let statusTS = dayjs(val['last_detected']);
                    let since = dayjs.duration(statusTS.diff(dayjs()));

                    // threshold for "red" is 2 minutes
                    if (since.asMinutes() <= -2) {
                        val['_rowVariant'] = 'danger';
                    } else {
                        val['_rowVariant'] = 'warning';
                    }
                    val['since'] = since.humanize(true);
                    return val;
                });
            },
        },
        methods: {
            showCode: function(code, button) {
                this.codeModal.content = code;
                this.$root.$emit('bv::show::modal', this.codeModal.id, button)
            },
            resetModal: function() {
                this.codeModal.content = '';
            }
        },
        template: `
            <div>
                <b-table hover 
                    :fields="fields" 
                    :items="items"
                    :sort-by.sync="sortBy"
                    :sort-desc.sync="sortDesc"
                    >
                        <template v-slot:cell(implementation)="row">
                            <b-button size="sm" @click="showCode(row.item.code, $event.target)">
                                Show Code
                            </b-button>
                       </template>
                    </b-table>
                <b-modal :id="codeModal.id" size="lg" ok-only @hide="resetModal">
                    <pre>{{ codeModal.content }}</pre>
                </b-modal>
            </div>
        `
    });

    dayjs.extend(window.dayjs_plugin_duration);
    dayjs.extend(window.dayjs_plugin_relativeTime);
    const app = new Vue({
        el: '#app',
        data: {
            statuses: [],
            building: '<loading>',
            worldtime: dayjs(),
        },
        methods: {
            poll: function() {
                fetch('/get_status')
                    .then(resp => resp.json())
                    .then(data => {
                        console.log(data);
                        this.statuses = data.statuses;
                        this.building = data.building;
                        this.worldtime = dayjs(data.time);
                    })
            },
        },
        created () {
            this.poll();
            setInterval(this.poll, 1000);
        }
    });
})
