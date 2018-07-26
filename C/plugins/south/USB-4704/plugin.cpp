/*
 * FogLAMP south plugin.
 *
 * Copyright (c) 2018 Dianomic Systems
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Mark Riddoch
 */
#include <usb4704.h>
#include <plugin_api.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <string>
#include <logger.h>
#include <plugin_exception.h>
#include <config_category.h>
#include <rapidjson/document.h>

using namespace std;

/**
 * Default configuration
 */
#define CONFIG	"{\"plugin\" : { \"description\" : \"Advantech USB-4704 Data Acquisition Module\", " \
			"\"type\" : \"string\", \"default\" : \"usb-4704\" }, " \
		"\"asset\" : { \"description\" : \"Asset name to use for readings\", " \
			"\"type\" : \"string\", \"default\" : \"usb-4704\" }, " \
		"\"connections\" : { \"description\" : \"Utilisation of connections on USB-4704\", " \
			"\"type\" : \"JSON\", \"default\" : { " \
				"\"analogue_example\" : { " \
					"\"type\" : \"analogue\", " \
					"\"pin\" : \"AI0\", " \
					"\"name\" : \"value1\", " \
					"\"scale\" : 0.1 " \
				"}, " \
				"\"digital_example\" : { " \
					"\"type\" : \"example\", " \
					"\"pins\" : [\"DI0\", \"DI1\", \"DI2\", \"DI3\"], " \
					"\"name\" : \"value1\", " \
					"\"scale\" : 0.1 " \
				"} " \
			"} } }"

/**
 * The USB-4704 plugin interface
 */
extern "C" {

/**
 * The plugin information structure
 */
static PLUGIN_INFORMATION info = {
	"usb-4704",               // Name
	"1.0.0",                  // Version
	0,    			  // Flags
	PLUGIN_TYPE_SOUTH,        // Type
	"1.0.0",                  // Interface version
	CONFIG			  // Default configuration
};

/**
 * Return the information about this plugin
 */
PLUGIN_INFORMATION *plugin_info()
{
	return &info;
}

/**
 * Initialise the plugin, called to get the plugin handle
 */
PLUGIN_HANDLE plugin_init(ConfigCategory *config)
{
USB4704 *usb = 0;


	usb = new USB4704();

	if (config->itemExists("asset"))
	{
		usb->setAssetName(config->getValue("asset"));
	}
	else
	{
		usb->setAssetName("usb4704");
	}

	// Now process the Connections
	string connections = config->getValue("connections");
	rapidjson::Document doc;
	doc.Parse(connections.c_str());
	if (!doc.HasParseError())
	{
		for (rapidjson::Value::ConstMemberIterator itr = doc.MemberBegin();
						itr != doc.MemberEnd(); ++itr)
		{
			const char *type = itr->value["type"].GetString();
			if (strcmp(type, "analogue") == 0)
			{
				double scale = 1.0;
				if (itr->value.HasMember("scale"))
				{
					scale = itr->value["scale"].GetFloat();
				}
				if (itr->value.HasMember("pin") && itr->value["pin"].IsString())
				{
					usb->addAnalogueConnection(itr->name.GetString(), itr->value["pin"].GetString(),
							scale);
				}
				else
				{
					Logger::getLogger()->error("Analogue connection for USB-4704 is missing definition of pin");
					throw exception();
				}
			}
			else if (strcmp(type, "digital") == 0)
			{
				// TODO add support for digitial inputs
			}
			else
			{
				throw exception();
			}
		}
	}

	return (PLUGIN_HANDLE)usb;
}

/**
 * Start the Async handling for the plugin
 */
void plugin_start(PLUGIN_HANDLE *handle)
{
	if (!handle)
		return;
}

/**
 * Poll for a plugin reading
 */
Reading plugin_poll(PLUGIN_HANDLE *handle)
{
USB4704 *usb = (USB4704 *)handle;

	if (!handle)
		throw exception();
	return usb->takeReading();
}

/**
 * Reconfigure the plugin
 */
void plugin_reconfigure(PLUGIN_HANDLE *handle, string& newConfig)
{
}

/**
 * Shutdown the plugin
 */
void plugin_shutdown(PLUGIN_HANDLE *handle)
{
USB4704 *usb = (USB4704 *)handle;

	delete usb;
}
};
