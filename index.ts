
import { BusinessNetworkConnection } from 'composer-client';
import * as express from "express";
import { Application, NextFunction, Request, Response, Router } from "express";
import * as querystring from 'querystring';
import * as http from 'http';
import * as fs from 'fs';
import { createHash, Hash } from 'crypto';

import * as bodyParser from "body-parser";
import * as cookieParser from "cookie-parser";
import * as errorHandler from 'errorhandler';
import * as methodOverride from 'method-override';
import * as logger from "morgan";
import * as path from "path";

import * as passport from 'passport';
import { Strategy, ExtractJwt } from 'passport-jwt';

let DarkSky = require('dark-sky');
 


// Data Models
import { IClaim, IClaimModel, IClaimSubmission } from './shared/models/claim';
import { IWeatherData, IWeatherDataModel } from './shared/models/weatherData';



let RainOracleConfig = {
    RAINORACLE_CARD_NAME: 'rainOracle@black-poc',
    CHAINCODE_NAMESPACE: 'insure.black.poc',
    INSURANCE_AGENCY_ID: 'BLACK_INSURANCE',
    INSURANCE_PRODUCT_ID: 'RAINY_DAY_INSURANCE',
    CREATE_SCHEDULE_TIMER_BEFORE_NEXT_HOUR: 5*60*1000,
    EXECUTE_WEATHER_CHECK_SCHEDULE_INTERVAL: 60*1000,
    MINIMUM_WEATHER_RPM: 10,
    MAXIMUM_WEATHER_RPM: 150,
    DARK_SKY_API_KEY: '16692e24630fbd4ec2395d0265d0cb37',    
    CLAIM_BATCH_SIZE: 20,
    PROCESS_LOOP_INTERVAL_MS: 5000,
    RAIN_THRESHOLD_IN_MM_PER_24_HOURS_FOR_CLAIM: 10,
    JWT_AUTHORIZATION_SECRET: process.env.JWT_AUTHORIZATION_SECRET || 'secret'
};


/**
 * Microservice for checking weather for locations covered by all the active Policies, and submitting insurance Claims to the Blockchain
 * @class RainOracle
 */
export class RainOracle {

    private app: Application;

    private networkConnection?: BusinessNetworkConnection;
    private networkDefinition: any;

    private darksky : any;

    private weatherCheckingSchedule_lastHourCompleted: Date = new Date(Date.UTC((new Date()).getUTCFullYear(), (new Date()).getUTCMonth(), (new Date()).getUTCDate(), (new Date()).getUTCHours()-1));
    private weatherCheckingSchedule_current: IWeatherData[] = new Array();
    private weatherCheckingSchedule_next: IWeatherData[] = new Array();
    private weatherChecks_inProcess: IWeatherData[] = new Array();

    private locationPolicies: any = {};
    private locationPolicies_next: any = {};
    private locations_completedThisHour: string[] = new Array();

    private weatherClaims_pending: IClaimSubmission[] = new Array();
    private weatherClaims_inProcess: IClaimSubmission[] = new Array();

    private serviceConfig: any;

    private blockchainEventListener: any;
    private weatherCheckScheduleMakerTimer: any;
    private hourlyScheduleMaintenanceTimer: any;
    private claimSubmitterTimer: any;

    public HALT_LISTENING: boolean = true;
    public HALT_PROCESSING: boolean = true;


    /**
     * Bootstrap the application.
     *
     * @class RainOracle
     * @method bootstrap
     * @static
     * @return {ng.auto.IInjectorService} Returns the newly created injector for this app.
     */
    public static bootstrap(): RainOracle {
        return new RainOracle();
    }


    constructor() {    
        //create expressjs application
        this.app = express();

        this.config();

        this.routes();
    }


    /**
     * Configure application
     *
     * @class Server
     * @method config
     */
    public config() {
        //add static paths
        this.app.use(express.static(path.join(__dirname, "./public")));
    
        //use logger middlware
        this.app.use(logger("dev"));

        //enable CORS for different OAuth protocols between UI and server
        //this.app.use(cors());
    
        //use json form parser middlware
        this.app.use(bodyParser.json());
    
        //use query string parser middlware
        this.app.use(bodyParser.urlencoded({extended: true}));
    
        //use override middlware
        this.app.use(methodOverride());

        //catch 404 and forward to error handler
        this.app.use(function(err: any, req: express.Request, res: express.Response, next: express.NextFunction) {
            err.status = 404;
            next(err);
        });

        //error handling
        this.app.use(errorHandler());



        
        // prepare to connect to the blockchain and db
        this.networkConnection = new BusinessNetworkConnection();
        
        // retrieve the configuration for the micro-service
        this.serviceConfig = RainOracleConfig;
    
        try{
            // Connect to the Blockchain
            this.networkConnection.connect(this.serviceConfig.RAINORACLE_CARD_NAME)
            .then( (connectedNetworkDefinition: any) => {
                this.networkDefinition = connectedNetworkDefinition;
            });

            // Create the DarkSky connection
            this.darksky = new DarkSky(this.serviceConfig.DARK_SKY_API_KEY);

            // Load the ability to understand / communicate JWT in Passport for request authorisation 
            passport.use(new Strategy({
                jwtFromRequest: ExtractJwt.fromAuthHeaderAsBearerToken(),
                secretOrKey: this.serviceConfig.JWT_AUTHORIZATION_SECRET
            },
            (jwtPayload:any, cb:any) => { return cb(null, jwtPayload); }
            ));

        } catch (err) {
            console.log('Failed to configure connections with the Blockchain and Database');
            throw err;
        }
    }

    /**
     * Create router
     *
     * @class RainOracle
     * @method routes
     */
    public routes() {
        //add default route, to get current status
        this.app.get('/', passport.authenticate('jwt', {session:false}), (req: Request, res: Response, next: NextFunction) => {
            this.status(req, res, next);
        });

        //add start processing route
        this.app.post('/start', passport.authenticate('jwt', {session:false}), (req: Request, res: Response, next: NextFunction) => {
            this.startProcessing(req, res, next);
            this.status(req, res, next);
        });

        //add stop processing route
        this.app.post('/stop', passport.authenticate('jwt', {session:false}), (req: Request, res: Response, next: NextFunction) => {
            this.stopProcessing(req, res, next);
            this.status(req, res, next);
        });
    }






    /**
     * The status route.
     *
     * @class RainOracle
     * @method status
     * @param req {any} The express Request object.
     * @param res {Response} The express Response object.
     * @next {NextFunction} Execute the next method.
     */
    private status(req: any, res: Response, next: NextFunction) {
        return res.json({
            "weatherCheckProcessor_running": (!this.HALT_PROCESSING), 
            "blockchainListener_running" : (!this.HALT_LISTENING),
            "oracleTime_current":(new Date()).toTimeString(),
            "numberOfWeatherChecks": {
                "remainingInCurrentHour":(this.weatherCheckingSchedule_current.length + this.weatherChecks_inProcess.length),
                "nextHour":this.weatherCheckingSchedule_next.length,
                "inProcess":this.weatherChecks_inProcess.length
            },
            "weatherClaims" : {
                "pending":this.weatherClaims_pending.length,
                "inProcess":this.weatherClaims_inProcess.length
            }
        });
    }

    /**
     * The startProcessing route.
     *
     * @class RainOracle
     * @method startProcessing
     * @param req {any} The express Request object.
     * @param res {Response} The express Response object.
     * @next {NextFunction} Execute the next method.
     */
    private startProcessing(req: any, res: Response, next: NextFunction) {
        this.setListenForBlockchainEvents(true);

        this.HALT_PROCESSING = false;
        this.weatherCheckingSchedule_lastHourCompleted = this.getHourDate(-1);
        this.weatherCheckingSchedule_current = new Array();
        this.weatherCheckingSchedule_next = new Array();
        this.weatherChecks_inProcess = new Array();
        this.locations_completedThisHour = new Array();
        this.weatherClaims_pending = new Array();
        this.weatherClaims_inProcess = new Array()

        this.debug('IMMEDIATE', 'startProcessing', 'loadConfigFromBlockchain');
        setImmediate(()=>{return this.loadConfigFromBlockchain();});

        this.debug('IMMEDIATE', 'startProcessing', 'executeWeatherCheckingSchedule');
        setImmediate(()=>{return this.executeWeatherCheckingSchedule();});

        // First time we start the schedule maker for next hour, we need to compute how many milliseconds from now the first execution should start
        // Computed as MILLISECONDS_FROM_NOW_TIL_END-OF-HOUR - CREATE_SCHEDULE_TIMER_BEFORE_NEXT_HOUR
        let now = new Date();
        let endOfHour = new Date(now.getFullYear(), now.getMonth(), now.getDate(), now.getHours());
        endOfHour.setHours(endOfHour.getHours()+1);
        let millisecondsTilEndOfHour = (endOfHour.getTime() - now.getTime());
        let timerMilliseconds = millisecondsTilEndOfHour - this.serviceConfig.CREATE_SCHEDULE_TIMER_BEFORE_NEXT_HOUR;
        if (timerMilliseconds < 0){ timerMilliseconds = timerMilliseconds + (60*60*1000); }

        // Create a timer that will create an interval, so once we are at CREATE_SCHEDULE_TIMER_BEFORE_NEXT_HOUR, we can just run same time every hour
        this.debug('TIMEOUT', 'startProcessing', 'weatherCheckScheduleMakerTimer', timerMilliseconds);
        this.weatherCheckScheduleMakerTimer = setTimeout(()=>{
            this.debug('CALLBACK', 'startProcessing', 'weatherCheckScheduleMakerTimer', timerMilliseconds);
            if ( this.HALT_PROCESSING ) { return; }

            this.weatherCheckScheduleMakerTimer = setInterval(()=>{
                this.debug('INTERVAL', 'weatherCheckScheduleMakerTimer', 'weatherCheckScheduleMakerTimer', 60*60*1000);
                if ( this.HALT_PROCESSING ) { return; }                
                return this.createNextWeatherCheckSchedule()
                            .then( (newSchedule) => {
                                if ( this.HALT_PROCESSING ) { return Promise.reject("Processor is shutting down"); }
                                if (newSchedule.length > 0){ this.weatherCheckingSchedule_next = newSchedule; }
                                this.debug('CALLBACK-I', 'weatherCheckScheduleMakerTimer', 'createNextWeatherCheckSchedule');
                                return Promise.resolve(true);
                            }).catch( (scheduleError) => {
                                console.log('Failed to load the WeatherCheck schedule from the Blockchain');
                                console.log(scheduleError);
                                return Promise.reject(scheduleError);
                            });
                }, 60*60*1000);

            return this.createNextWeatherCheckSchedule()
                        .then( (newSchedule) => {
                            if ( this.HALT_PROCESSING ) { return Promise.reject("Processor is shutting down"); }
                            if (newSchedule.length > 0){ this.weatherCheckingSchedule_next = newSchedule; }
                            this.debug('CALLBACK-T', 'weatherCheckScheduleMakerTimer', 'createNextWeatherCheckSchedule');
                            return Promise.resolve(true);
                        }).catch( (scheduleError) => {
                            console.log('Failed to load the WeatherCheck schedule from the Blockchain');
                            console.log(scheduleError);
                            return Promise.reject(scheduleError);
                        });
        }, timerMilliseconds);
        
        // Create the timer that will reset hourly progress measures and merge the next schedule into the current schedule
        this.hourlyScheduleMaintenanceTimer = setTimeout(() => {
            if ( this.HALT_PROCESSING ) { return; }

            this.hourlyScheduleMaintenanceTimer = setInterval(()=>{
                if ( this.HALT_PROCESSING ) { return; }
                // Merge the next schedule into the current schedule, then reset the next schedule and locations-retrieved-this-hour
                this.weatherCheckingSchedule_current = this.weatherCheckingSchedule_current.concat(this.weatherCheckingSchedule_next);
                this.weatherCheckingSchedule_next = new Array();
                this.locations_completedThisHour = new Array();
                this.locationPolicies = this.locationPolicies_next;
                this.locationPolicies_next = new Array();
                this.debug('INTERVAL', 'startProcessing', 'hourlyScheduleMaintenanceTimer', 60*60*1000);
            }, 60*60*1000);

            // Merge the next schedule into the current schedule, then reset the next schedule and locations-retrieved-this-hour
            this.weatherCheckingSchedule_current = this.weatherCheckingSchedule_current.concat(this.weatherCheckingSchedule_next);
            this.weatherCheckingSchedule_next = new Array();
            this.locations_completedThisHour = new Array();
            this.locationPolicies = this.locationPolicies_next;
            this.locationPolicies_next = new Array();
            this.debug('TIMEOUT', 'startProcessing', 'hourlyScheduleMaintenanceTimer', millisecondsTilEndOfHour);
        }, millisecondsTilEndOfHour);

        // Create the timer that will submit claims as they are identified by the weather checker
        this.claimSubmitterTimer = setTimeout(() => {
            if ( this.HALT_PROCESSING ) { return; }

            this.claimSubmitterTimer = setInterval(()=>{
                if ( this.HALT_PROCESSING ) { return; }
                this.debug('INTERVAL', 'startProcessing', 'claimSubmitterTimer', 1*60*1000);                
                if ( this.weatherClaims_pending.length > 0 ) {return this.processPendingClaims();}
            }, 1*60*1000);
            this.debug('TIMEOUT', 'startProcessing', 'claimSubmitterTimer', 3*60*1000);
            if ( this.weatherClaims_pending.length > 0 ) {return this.processPendingClaims(); }
        }, 3*60*1000);

    }

    /**
     * The stopProcessing route.
     *
     * @class RainOracle
     * @method stopProcessing
     * @param req {any} The express Request object.
     * @param res {Response} The express Response object.
     * @next {NextFunction} Execute the next method.
     */
    private stopProcessing(req: any, res: Response, next: NextFunction) {
        this.HALT_PROCESSING = true;
        this.setListenForBlockchainEvents(false);
        clearInterval(this.weatherCheckScheduleMakerTimer);
        clearInterval(this.hourlyScheduleMaintenanceTimer);
        clearInterval(this.claimSubmitterTimer);
        this.debug("GLOBALS", "stopProcessing", "SHUTTING DOWN", 0)
    }


    private setListenForBlockchainEvents(shouldListenForBlockchainEvents: boolean = true){
        if ( this.networkConnection == undefined ){ return; }

        if ( shouldListenForBlockchainEvents ) {
            this.HALT_LISTENING = false;
            this.blockchainEventListener = this.networkConnection.on('event', (evt: any) => {
                this.processBlockchainEvent(evt)
                    .then( (success: any) => {
                        if (!success){
                            console.log('WARNING: Did not process a captured event');
                        }
                    }).catch( (eventError: any) => {
                        console.log('ERROR: failed to process an event');
                        console.log(eventError);
                    });
            });
            this.networkConnection.on('error', (blockchainError: any) => {
                console.log('ERROR: received an error from the Blockchain');
                console.log(blockchainError);
            });
        } else {
            this.networkConnection.removeAllListeners();
            this.blockchainEventListener = null;
            this.HALT_LISTENING = true;
        }
    }




    private loadConfigFromBlockchain() : Promise<void> {
        return this.getRainThresholdFromBlockchain(this.serviceConfig.INSURANCE_AGENCY_ID)
                .then( (rainThreshold) => {
                    this.serviceConfig.RAIN_THRESHOLD_IN_MM_PER_24_HOURS_FOR_CLAIM = rainThreshold;
                    return Promise.resolve();
                }).catch( (blockchainRetrieveError) => {
                    console.log('Failed to retrieve the Insurance Agency config from the Blockchain');
                    console.log(blockchainRetrieveError);
                    return Promise.reject(blockchainRetrieveError);
                });
    }


    private executeWeatherCheckingSchedule() : Promise<any>{
        this.debug('ENTRY', 'executeWeatherCheckingSchedule', '');
        if ( this.HALT_PROCESSING ) { return Promise.resolve(true); }

        // Determine if everything is finished for the current hour
        let currentHourDate = this.getHourDate();
        let nextHourDate = this.getHourDate(1);
        if( this.weatherCheckingSchedule_lastHourCompleted == currentHourDate ){
            // Nothing to do, sleep for a while and check again
            this.debug('TIMEOUT', 'executeWeatherCheckingSchedule', 'executeWeatherCheckingSchedule', 5*60*1000);
            setTimeout(()=>{return this.executeWeatherCheckingSchedule();}, 5*60*1000);
            Promise.resolve(true);
        }

        // If the schedule is not populated, this is a first time run
        if ( this.weatherCheckingSchedule_current.length == 0 ){
            return this.createCurrentWeatherCheckSchedule()
                .then( (newSchedule) => {
                    if ( this.HALT_PROCESSING ) { return Promise.reject("Processor is shutting down"); }

                    if (newSchedule.length == 0){
                        // No active policies needing weather checks
                        this.weatherCheckingSchedule_lastHourCompleted = currentHourDate;
                        this.debug('TIMEOUT', 'executeWeatherCheckingSchedule', 'executeWeatherCheckingSchedule', 5*60*1000);
                        setTimeout(()=>{return this.executeWeatherCheckingSchedule();}, 5*60*1000);
                    } else {
                        // Set the current schedule and rerun execution of the weather check schedule
                        this.weatherCheckingSchedule_current = newSchedule;
                        this.debug('IMMEDIATE', 'executeWeatherCheckingSchedule', 'executeWeatherCheckingSchedule');
                        setImmediate(()=> {return this.executeWeatherCheckingSchedule();});
                    }
                    return Promise.resolve(true);
                }).catch((scheduleError)=>{
                    console.log('Failed to load the WeatherCheck schedule from the Blockchain');
                    console.log(scheduleError);
                    return Promise.reject(scheduleError);
                });
        } else {
            // Run this again in 60 seconds
            this.debug('TIMEOUT', 'executeWeatherCheckingSchedule', 'executeWeatherCheckingSchedule', 60*1000);
            setTimeout(()=>{return this.executeWeatherCheckingSchedule();}, 60*1000);

            // Get the current minute
            let currentMinuteDate = this.getHourDate();
            currentMinuteDate.setMinutes((new Date()).getUTCMinutes());

            // Create a list of requests that need to go out this minute
            let currentRequestPromises : Promise<IWeatherData>[] = new Array();

            let weatherScheduleStartTime : Date = new Date(Date.parse(this.weatherCheckingSchedule_current[0].timeRecordedISOString));
            while(this.weatherCheckingSchedule_current.length > 0 && currentMinuteDate.getTime() >= weatherScheduleStartTime.getTime()){
                let currentWeatherCheck = this.weatherCheckingSchedule_current.shift();
                if (currentWeatherCheck == undefined){ break; }
                this.weatherChecks_inProcess.push(currentWeatherCheck);

                this.debug('LOOP', 'executeWeatherCheckingSchedule', 'makeWeatherRequest');

                currentRequestPromises[currentRequestPromises.length] = this.makeWeatherRequest(currentWeatherCheck);
            }

            if (currentRequestPromises.length == 0){
                this.debug('NOOP', 'executeWeatherCheckingSchedule', 'currentRequestPromises.length == 0');
                return Promise.resolve(true);
            }

            // Wait for all the Weather requests to come back
            return Promise.all(currentRequestPromises)
                .then( (weatherResponses:IWeatherData[]) => { 
                    this.debug('CALLBACK', 'executeWeatherCheckingSchedule', 'weatherResponses');

                    // Loop through the weather reponses, looking for any locations that received more than the rain threshold in the last 24 hours
                    let locations_completedThisHour_thisRun : string[] = new Array();
                    let weatherClaims_pending_thisRun : IClaimSubmission[] = new Array();
                    for(let i = 0; i < weatherResponses.length; i++){
                        locations_completedThisHour_thisRun[locations_completedThisHour_thisRun.length] = weatherResponses[i].weatherLocation;

                        if (weatherResponses[i].rainLast24Hours > this.serviceConfig.RAIN_THRESHOLD_IN_MM_PER_24_HOURS_FOR_CLAIM) {
                            let policiesPerLocation = this.locationPolicies[this.getLocationAlias(weatherResponses[i].weatherLocation)];

                            for(let j = 0; j < policiesPerLocation.length; j++){
                                weatherClaims_pending_thisRun[weatherClaims_pending_thisRun.length] = {
                                    policyID : policiesPerLocation[j],
                                    rainLast24Hours : weatherResponses[i].rainLast24Hours,
                                    cloudsLast24Hours : weatherResponses[i].cloudsLast24Hours,
                                    highTempLast24Hours : weatherResponses[i].highTempLast24Hours,
                                    highWaveLast24Hours : weatherResponses[i].highWaveLast24Hours
                                };
                            }
                        }
                    }

                    // Append the completed locations with the main list
                    this.locations_completedThisHour = this.locations_completedThisHour.concat(locations_completedThisHour_thisRun);

                    // Append the new pending Claims to the main list
                    this.weatherClaims_pending = this.weatherClaims_pending.concat(weatherClaims_pending_thisRun);

                    // Remove the completed locations from the inProcess list
                    this.weatherChecks_inProcess = this.weatherChecks_inProcess.filter( (value,index,self) => {
                        return (locations_completedThisHour_thisRun.indexOf(value.weatherLocation) == -1);
                    });

                    this.debug('GLOBALS', 'executeWeatherCheckingSchedule', 'weatherResponses');

                    Promise.resolve(true);
                });
        }
    }





    private processPendingClaims() : Promise<boolean> {
        if ( this.networkConnection == undefined ){
            console.log('WARNING: The Network Connection is not initialized.');
            return Promise.reject('WARNING: The Network Connection is not initialized.');
        }

        let factory = this.networkDefinition.getFactory();
        let claimBatchSize = ( this.serviceConfig.CLAIM_BATCH_SIZE < this.weatherClaims_pending.length ) ? this.serviceConfig.CLAIM_BATCH_SIZE : this.weatherClaims_pending.length;
        let submitClaimTransactions = new Array(claimBatchSize);
        let submitClaimPromises = new Array(claimBatchSize);
        this.debug('MESSAGE', 'processPendingClaims', 'Submitting new Claims to the Blockchain', claimBatchSize);
        
        // Loop through the current pending claims up to the batch size limit (keep grabbing index 0, shifting it at the beginning of the loop, for the entire batch)
        for(let i = 0; i < claimBatchSize; i++){
            if (this.HALT_PROCESSING) { return Promise.resolve(true); }

            let currentPendingClaim = this.weatherClaims_pending.shift(); 
            if (currentPendingClaim == undefined) { continue; }   

            // Create a new 'SubmitClaim' transaction and add it to the current batch
            submitClaimTransactions[i] = factory.newResource(this.serviceConfig.CHAINCODE_NAMESPACE, 'SubmitClaim', (new Date()).getTime().toString()+'_'+currentPendingClaim.policyID);
            submitClaimTransactions[i].policyID = currentPendingClaim.policyID;
            submitClaimTransactions[i].rainLast24Hours = currentPendingClaim.rainLast24Hours;
            submitClaimTransactions[i].cloudsLast24Hours = currentPendingClaim.cloudsLast24Hours;
            submitClaimTransactions[i].highTempLast24Hours = currentPendingClaim.highTempLast24Hours;
            submitClaimTransactions[i].highWaveLast24Hours = currentPendingClaim.highWaveLast24Hours;

            // Move the claim to the in-process queue
            this.weatherClaims_inProcess[this.weatherClaims_inProcess.length] = currentPendingClaim;   
        }

        // Execute each 'SubmitClaim' transaction individually on the Blockchain                    
        for(let j = 0; j < submitClaimPromises.length; j++){
            submitClaimPromises[j] = this.networkConnection.submitTransaction(submitClaimTransactions[j]);
        }

        return Promise.all(submitClaimPromises)
            .then( (createdPolicies) => {
                return Promise.resolve(true);
            }).catch( (pendingClaimSyncExecutionError) => {
                console.log('Failed to execute the sync of Pending Claims.');
                console.log(pendingClaimSyncExecutionError);
                return Promise.reject(pendingClaimSyncExecutionError);
            });
    }



    private processBlockchainEvent(eventDetails: any){
        let eventType = eventDetails.$type;
        switch (eventType){
            case 'NewPolicyIssued':
                console.log('Received a NewPolicyIssued event');
                return Promise.resolve(false);
            case 'ClaimSubmitted':
                this.debug('MESSAGE', 'processBlockchainEvent', 'Received a ClaimSubmitted event');

                if (this.networkConnection == undefined){return Promise.reject('Blockchain connection has not been initialized.');}

                // Create a new 'SettleClaim' transaction 
                let factory = this.networkDefinition.getFactory();
                var settleClaimTransaction = factory.newResource(this.serviceConfig.CHAINCODE_NAMESPACE, 'SettleClaim', 'FINALIZED_'+eventDetails.settlementPaymentID);
                settleClaimTransaction.policyID = eventDetails.policyID;
                settleClaimTransaction.claimID = eventDetails.claimID;
                settleClaimTransaction.settlementPaymentID = eventDetails.settlementPaymentID;
                settleClaimTransaction.policyHolderID = eventDetails.paidTo;
                settleClaimTransaction.policyUnderwriterID = eventDetails.paidFrom;
                
                return this.networkConnection.submitTransaction(settleClaimTransaction);
            case 'ClaimSettled':
                this.debug('MESSAGE', 'processBlockchainEvent', 'Received a ClaimSettled event');

                this.weatherClaims_inProcess = this.weatherClaims_inProcess.filter((value,index,self)=>{return value.policyID!=eventDetails.policyID;});
                
                return Promise.resolve(true);
            default:
                console.log('WARNING: Received an unknown event : ' + eventType);
                return Promise.resolve(false);
        }
    }



    


    private createCurrentWeatherCheckSchedule() : Promise<IWeatherData[]> {
        let currentHourDate = this.getHourDate();

        // Get a distinct set of locations and the related active policies for the scheduleDateHour
        return this.getActivePolicyLocationsNeedingWeatherCheck(currentHourDate)
            .then( (policiesNeedingWeatherCheck:any[]) => {
                // Eliminate locations we have already checked this hour
                this.debug('CALLBACK', 'createCurrentWeatherCheckSchedule', 'getActivePolicyLocationsNeedingWeatherCheck');
                policiesNeedingWeatherCheck = policiesNeedingWeatherCheck.filter( (policy,index,self) => {
                    return ( this.locations_completedThisHour.indexOf(policy.coveredCity) == -1 );
                }); 

                // Get the structured list of locationPolicies from those needing weather checks
                this.locationPolicies = this.convertPolicyListToCollection(policiesNeedingWeatherCheck);
                let locationsOnly : any[] = this.locationPolicies.onlyLocations;

                // Get the time of the last item in the current scheduled list, and compute the schedule start time ( last item +1 minute )
                let lastScheduledTime : Date = this.getHourDate(0, true);
                if (this.weatherCheckingSchedule_current.length > 0){
                    lastScheduledTime = new Date(Date.parse(this.weatherCheckingSchedule_current[this.weatherCheckingSchedule_current.length-1].timeRecordedISOString));
                }
                let scheduleStartTime : Date = new Date(lastScheduledTime);
                scheduleStartTime.setMinutes(lastScheduledTime.getMinutes()+1);

                // Convert the policies to a schedule
                let schedule : IWeatherData[] = this.createWeatherCheckSchedule(locationsOnly, scheduleStartTime);
                return Promise.resolve(schedule);
            }).catch( (scheduleBuildingError:any) => {
                console.log('Failed to build the weather checking schedule.');
                console.log(scheduleBuildingError);
                return Promise.reject(scheduleBuildingError);
            });
    }


    private createNextWeatherCheckSchedule() : Promise<IWeatherData[]> {
        let nextHourDate = this.getHourDate(1)

        // Get a distinct set of locations and the related active policies for the nextHourDate
        return this.getActivePolicyLocationsNeedingWeatherCheck(nextHourDate)
            .then( (policiesNeedingWeatherCheck:any[]) => {
                this.debug('CALLBACK', 'createNextWeatherCheckSchedule', 'getActivePolicyLocationsNeedingWeatherCheck');

                // Get the structured list of locationPolicies from those needing weather checks
                this.locationPolicies_next = this.convertPolicyListToCollection(policiesNeedingWeatherCheck);
                let locationsOnly : any[] = this.locationPolicies_next.onlyLocations;

                // Get the time of the last item in the current scheduled list, and compute the schedule start time ( last item +1 minute )
                let lastScheduledTime : Date = new Date(nextHourDate);
                if (this.weatherCheckingSchedule_current.length > 0){
                    let currentScheduleLastTime = new Date(Date.parse(this.weatherCheckingSchedule_current[this.weatherCheckingSchedule_current.length-1].timeRecordedISOString));
                    if ( currentScheduleLastTime.getTime() > nextHourDate.getTime()) {
                        lastScheduledTime = currentScheduleLastTime;
                    }
                }
                let scheduleStartTime : Date = new Date(lastScheduledTime);
                scheduleStartTime.setMinutes(lastScheduledTime.getMinutes()+1);        

                // Convert the policies to a schedule
                let schedule : IWeatherData[] = this.createWeatherCheckSchedule(locationsOnly, scheduleStartTime);
                return Promise.resolve(schedule);
            }).catch( (scheduleBuildingError:any) => {
                console.log('Failed to build the future weather checking schedule.');
                console.log(scheduleBuildingError);
                return Promise.reject(scheduleBuildingError);
            });
    }
    

    private createWeatherCheckSchedule(locationList:any[], scheduleStartTime:Date) : IWeatherData[] {
        // Determine how many minutes are available in the hour after the scheduleStartTime
        let nextDateHour : Date = new Date(scheduleStartTime.getFullYear(), scheduleStartTime.getMonth(), scheduleStartTime.getDate(), scheduleStartTime.getHours());
        nextDateHour.setHours(nextDateHour.getHours()+1);
        let remainingMinutes : number = ((nextDateHour.getTime() - scheduleStartTime.getTime()) / 1000);

        // Determine the amount of Requests to schedule per minute - above the minimum Request/minute and below the max Request/minute
        let scheduleRPM : number = this.serviceConfig.MINIMUM_WEATHER_RPM;
        let actualRPM = (locationList.length / remainingMinutes);
        if (actualRPM > this.serviceConfig.MINIMUM_WEATHER_RPM) {
            if ( actualRPM > this.serviceConfig.MAXIMUM_WEATHER_RPM ) {
                scheduleRPM = this.serviceConfig.MAXIMUM_WEATHER_RPM;
            } else {
                scheduleRPM = actualRPM;
            }
        }
        let requiredMinutes = Math.ceil(locationList.length / scheduleRPM);

        // Create the weather schedule
        let schedule : IWeatherData[] = new Array();
        let currentScheduleMinute : number = scheduleStartTime.getMinutes();
        for(let i = 0; i < locationList.length; i+=scheduleRPM){
            let scheduledMinute = new Date(scheduleStartTime);
            scheduledMinute.setMinutes(currentScheduleMinute);
            for(let j = 0; j < scheduleRPM && i+j < locationList.length; j++){
                schedule[i+j] = {
                    timeRecordedISOString: scheduledMinute.toISOString(),
                    weatherLocation: locationList[i+j].coveredCity,
                    latitude: locationList[i+j].latitude,
                    longitude: locationList[i+j].longitude,
                    highTempLast24Hours: 0,
                    rainLast24Hours: 0,
                    cloudsLast24Hours: 0,
                    highWaveLast24Hours: 0
                };
            }
            currentScheduleMinute++;
        }
        return schedule;
    }


    /**
     * Retrieve the RainThreshold configured by the InsuranceAgency for automatic Claim settlement
     * Default values will be used if this value is not available from the Blockchain
     * Threshold = Millimeters of rain in a 24-hour period.  If rains more than this threshold, you have a valid Claim
     */
    private getRainThresholdFromBlockchain(insuranceAgencyID:string) : Promise<number> {
        let defaultThreshold = this.serviceConfig.RAIN_THRESHOLD_IN_MM_PER_24_HOURS_FOR_CLAIM;

        if (this.networkConnection == undefined){
            return Promise.resolve(defaultThreshold);
        }

        // Get an InsuranceAgency by ID  
        return this.networkConnection.query('InsuranceAgencyByID', {insuranceAgencyID: insuranceAgencyID})
            .then(function (insuranceAgency: any[]) {
                if ( insuranceAgency == null || insuranceAgency.length == 0 ){
                    return Promise.resolve(defaultThreshold);
                }

                return Promise.resolve(insuranceAgency[0].policyClaimRainThreshold);
            }).catch(function (blockchainLookupError:any) {
                console.log('Failed to query the Blockchain, looking for the main Insurance Agency.');
                console.log(blockchainLookupError);
                return Promise.reject(blockchainLookupError);
            });
    }


    private getActivePolicyLocationsNeedingWeatherCheck(dateHour:Date) : Promise<any[]> {
        if (this.networkConnection == undefined){
            return Promise.resolve(new Array());
        }

        let currentHourDate = this.convertDateToUTC(dateHour);
        let yesterdayHourDate = new Date(currentHourDate);
        yesterdayHourDate.setDate(currentHourDate.getDate()-1);

        // Get a distinct set of locations and the related active policies for the scheduleDateHour  
        return this.networkConnection.query('PoliciesNeedingWeatherCheck')
            .then(function (policyLocations: any[]) {
                // Eliminate locations that are expired OR have not started yet OR (have claims AND had a claim in last 24 hours)
                policyLocations = policyLocations.filter( (policy,index,self) => {
                    return (
                        (new Date(Date.parse(policy.startDateISOString))).getTime() < currentHourDate.getTime() && 
                        (new Date(Date.parse(policy.endDateISOString))).getTime() > currentHourDate.getTime() &&
                        !(  
                            false ||
                            (   policy.claims != undefined && 
                                policy.claims.length > 0 && 
                                (new Date(Date.parse(policy.lastClaimDateISOString))).getTime() > yesterdayHourDate.getTime()
                            )
                        )
                    );
                });

                return Promise.resolve(policyLocations);
            }).catch(function (blockchainLookupError:any) {
                console.log('Failed to query the Blockchain for Policies needing a Weather Check.');
                console.log(blockchainLookupError);
                return Promise.reject(blockchainLookupError);
            });
    }


    private makeWeatherRequest(scheduledRequest: IWeatherData) : Promise<IWeatherData> {
        let yesterday = new Date();
        yesterday.setDate(yesterday.getDate()-1);

        return this.darksky.options({
                latitude: scheduledRequest.latitude,
                longitude: scheduledRequest.longitude,
                time: yesterday,
                language: 'en',
                exclude: ['minutely', 'currently']
            })
            .get()
            .then( (response:any) => {
                scheduledRequest.timeRecordedISOString = (new Date()).toISOString();
                scheduledRequest.cloudsLast24Hours = response.daily.data[0].cloudCover;
                scheduledRequest.highTempLast24Hours = response.daily.data[0].temperatureHigh;

                // Compute the accumulated rain over the last 24 hours
                let accumulatedRain = 0;
                if (response.hourly.data.length > 0){
                    for(let i = 0; i < response.hourly.data.length; i++){
                        accumulatedRain += response.hourly.data[i].precipIntensity;
                    }
                    // Convert rain in inches to millimeters
                    console.log('DARKSKY: LAT ' + scheduledRequest.latitude + 
                                        ', LNG ' + scheduledRequest.longitude +
                                        ', RAIN_in ' + accumulatedRain + 
                                        ', RAIN_mm ' + (accumulatedRain * 25.4));
                    accumulatedRain = (accumulatedRain) * 25.4;
                }

                scheduledRequest.rainLast24Hours = accumulatedRain;

                return Promise.resolve(scheduledRequest);
            }).catch( (weatherRetrieveError:any) => {
                console.log('Failed to retrieve weather data for the current schedule location.');
                console.log(weatherRetrieveError);
                return Promise.reject(weatherRetrieveError);
            });

    }








    private convertPolicyListToCollection(policyList:any[]) : any {
        let result: any = {
            onlyLocations: []
        };

        // Create the distinct list of locations
        let allLocationNames = policyList.map<string>( (individualPolicyLocation) => {return individualPolicyLocation['coveredCity'];} );
        let distinctLocationNames = allLocationNames.filter( (value, index, self) => { return self.indexOf(value) === index; } );
        
        // Assemble the Policies per Location
        for(let i = 0; i < distinctLocationNames.length; i++){
            // Get the list of policies for the current location
            let locationPolicies = policyList.filter( (value, index, self) => { return value.coveredCity == distinctLocationNames[i]; });
            let policiesForLocation = locationPolicies.map<string>( (individualPolicyLocation) => {return individualPolicyLocation['policyID'].toString();} );
            
            result[this.getLocationAlias(distinctLocationNames[i])] = policiesForLocation;

            result.onlyLocations[result.onlyLocations.length] = {
                alias: this.getLocationAlias(distinctLocationNames[i]),
                coveredCity: distinctLocationNames[i],
                latitude: locationPolicies[0].latitude,
                longitude: locationPolicies[0].longitude
            };
        }

        return result;
    }



    private getHourDate(hourRelativeToNow:number = 0, precise:boolean = false) : Date {        
        let now = new Date();
        let currentHourDate : Date;
        
        if ( precise ) {
            currentHourDate = new Date();
        } else {
            currentHourDate = new Date(now.getFullYear(), now.getMonth(), now.getDate(), now.getHours());
        }

        if (hourRelativeToNow == 0){
            return currentHourDate;
        } else {
            let shiftedHourDate = currentHourDate;
            shiftedHourDate.setHours(currentHourDate.getHours()+hourRelativeToNow);
            return shiftedHourDate;
        }
    }

    private convertDateToUTC(originalDate : Date) : Date {
        return new Date(Date.UTC(originalDate.getUTCFullYear(),
                                originalDate.getUTCMonth(),
                                originalDate.getUTCDate(),
                                originalDate.getUTCHours(),
                                originalDate.getUTCMinutes(),
                                originalDate.getUTCSeconds(),
                                originalDate.getUTCMilliseconds()));
    }

    private getLocationAlias(location:string) : string {
        return createHash('sha1').update(location).digest('base64');
    }


    private mergeArrayDistinct(i:any[],x:any[]){let h:any={};let n:any[]=[];for(let a=2;a--;i=x)i.map((b)=>{h[b]=h[b]||n.push(b)});return n}


    private debug(timerType:string, sourceFunction:string, destinationFunction:string, delay:number = 0){
        console.log('DEBUG - ' + timerType + ' - ' + sourceFunction + ' --> ' + destinationFunction + ' (' + delay + ')');

        let summaryString = this.weatherCheckingSchedule_lastHourCompleted.toTimeString() + ' - ' + 
                            this.weatherCheckingSchedule_current.length + ' - ' + 
                            this.weatherCheckingSchedule_next.length + ' - ' + 
                            this.weatherChecks_inProcess.length + ' - ' + 
                            this.locations_completedThisHour.length + ' - ' + 
                            this.weatherClaims_pending.length + ' - ' + 
                            this.weatherClaims_inProcess.length;
        console.log(summaryString);
    }


}