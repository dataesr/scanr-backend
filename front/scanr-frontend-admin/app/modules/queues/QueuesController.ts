import CoreStatus = api.CoreStatus;

export class QueuesController {
    public lastUpdated: Date;
    public service:api.CoreStatusApi;
    public interval;
    public state:ng.ui.IStateService;
    public status: CoreStatus;
    public queues;

    constructor(CoreStatusApi: api.CoreStatusApi, $state: ng.ui.IStateService){
        "ngInject";
        this.queues = JSON.parse(require("./assets/queues.json"));
        this.state = $state;
        this.service = CoreStatusApi;
        this.lastUpdated = null;

        this.queueStatus();
        this.interval = setInterval(this.queueStatus.bind(this), 5000);
    }

    private queueStatus() {
        if (!this.state.includes("main.queues")) {
            clearInterval(this.interval);
            return;
        }
        this.service.status().then(function (data) { // [SWORD] becauseof newer Angular, else error success() is not a function https://stackoverflow.com/questions/33531336/angularjs-error-success-is-not-a-function
            this.lastUpdated = new Date();
            this.status = data;
        }.bind(this));
    };
}