package sustain.synopsis.samples.client.usgs;

import java.util.HashMap;
import java.util.Map;

public enum State {

    ALABAMA("Alabama","AL",4),
    ALASKA("Alaska","AK",9),
    ARIZONA("Arizona","AZ",4),
    ARKANSAS("Arkansas","AR",4),
    CALIFORNIA("California","CA",1),
    COLORADO("Colorado","CO",2),
    CONNECTICUT("Connecticut","CT",8),
    DELAWARE("Delaware","DE",13),
    FLORIDA("Florida","FL",1),
    GEORGIA("Georgia","GA",1),
    HAWAII("Hawaii","HI",5),
    IDAHO("Idaho","ID",5),
    ILLINOIS("Illinois","IL",1),
    INDIANA("Indiana","IN",2),
    IOWA("Iowa","IA",4),
    KANSAS("Kansas","KS",4),
    KENTUCKY("Kentucky","KY",2),
    LOUISIANA("Louisiana","LA",3),
    MAINE("Maine","ME",8),
    MARYLAND("Maryland","MD",3),
    MASSACHUSETTS("Massachusetts","MA",5),
    MICHIGAN("Michigan","MI",3),
    MINNESOTA("Minnesota","MN",7),
    MISSISSIPPI("Mississippi","MS",5),
    MISSOURI("Missouri","MO",2),
    MONTANA("Montana","MT",5),
    NEBRASKA("Nebraska","NE",4),
    NEVADA("Nevada","NV",5),
    NEW_HAMPSHIRE("New Hampshire","NH",16),
    NEW_JERSEY("New Jersey","NJ",2),
    NEW_MEXICO("New Mexico","NM",5),
    NEW_YORK("New York","NY",1),
    NORTH_CAROLINA("North Carolina","NC",1),
    NORTH_DAKOTA("North Dakota","ND",7),
    OHIO("Ohio","OH",1),
    OKLAHOMA("Oklahoma","OK",6),
    OREGON("Oregon","OR",2),
    PENNSYLVANIA("Pennsylvania","PA",2),
    RHODE_ISLAND("Rhode Island","RI",19),
    SOUTH_CAROLINA("South Carolina","SC",3),
    SOUTH_DAKOTA("South Dakota","SD",6),
    TENNESSEE("Tennessee","TN",6),
    TEXAS("Texas","TX",1),
    UTAH("Utah","UT",7),
    VERMONT("Vermont","VT",16),
    VIRGINIA("Virginia","VA",1),
    WASHINGTON("Washington","WA",3),
    WEST_VIRGINIA("West Virginia","WV",4),
    WISCONSIN("Wisconsin","WI",1),
    WYOMING("Wyoming","WY",11),

    DISTRICT_OF_COLUMBIA("District of Columbia","DC",31);

//    //    AMERICAN_SAMOA("American Samoa", "AQ", ""),
//    BRITISH_COLUMBIA("British Columbia", "BC", ""),
////    CANTON_AND_ENDERBURY_ISLANDS("Canton and Enderbury Islands", "EQ", ""),
//    DISTRICT_OF_COLUMBIA("District of Columbia","DC","US-DC");
////    GUAM("Guam", "GU", ""),
////    JOHNSTON_ATOLL("Johnston Atoll", "JQ", ""),
////    MIDWAY_ISLANDS("Midway Islands", "MQ", ""),
////    NORTHERN_MARIANA_ISLANDS("Northern Mariana Islands", "MP", ""),
////    PUERTO_RICO("Puerto Rico","PR","US-PR"),
////    RYUKYU_ISLANDS_SOUTHERN("Ryukyu Islands, Southern", "YQ", ""),
//////    SWAN_ISLANDS("Swan Islands", "SQ", ""),
////    TRUST_TERRITORIES_PACIFIC_IS("Trust Territories, Pacific Is", "TQ", ""),
////    US_MISC_CARIBBEAN_ISLANDS("U.S.Misc Caribbean Islands", "BQ", ""),
////    US_MISC_PACIFIC("U.S.Misc Pacific Islands", "IQ", ""),
////    VIRGIN_ISLANDS("Virgin Islands", "VI", ""),
////    WAKE_ISLAND("Wake Island", "WQ", "");

    private String unabbreviated;
    private String ANSIabbreviation;
    private int daysLength;

    State(String unabbreviated, String ANSIabbreviation, int daysLength) {
        this.unabbreviated = unabbreviated;
        this.ANSIabbreviation = ANSIabbreviation;
        this.daysLength = daysLength;
    }

    /**
     * The full, unabbreviated name of this state.
     */
    public String getUnabbreviated() {
        return this.unabbreviated;
    }

    /**
     * The ANSI abbreviated name of this state, e.g. "NY", or "WY".
     */
    public String getANSIAbbreviation() {
        return this.ANSIabbreviation;
    }

    public int getDaysLength() {
        return daysLength;
    }

}










