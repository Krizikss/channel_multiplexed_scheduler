import 'dart:io';

import 'package:venice_core/channels/channel_metadata.dart';
import 'package:venice_core/channels/events/bootstrap_channel_event.dart';
import 'package:venice_core/channels/abstractions/bootstrap_channel.dart';
import 'package:venice_core/channels/events/data_channel_event.dart';
import 'package:venice_core/channels/abstractions/data_channel.dart';
import 'package:venice_core/file/file_chunk.dart';
import 'package:venice_core/file/file_metadata.dart';
import 'package:flutter/material.dart';


/// The Receiver class goal is to receive a file from a Scheduler instance
/// that's running on another device.
class Receiver {
  final BootstrapChannel bootstrapChannel;
  late final List<DataChannel> _channels = [];
  final Map<int, FileChunk> _chunks = {};

  /// Number of chunks we expect to receive.
  /// Receiver will not end while it has not received expected chunks count.
  late int _chunksCount;

  /// Name of the file that will be created by the received.
  /// It is transmitted through the bootstrap channel.
  late String _filename;

  Receiver(this.bootstrapChannel);


  /// Adds a channel to use to receive data.
  void useChannel(DataChannel channel) {
    if (_channels.where((element) => element.identifier == channel.identifier).isNotEmpty) {
      throw ArgumentError('Channel identifier "${channel.identifier}" is already used.');
    }
    
    _channels.add(channel);
    channel.on = (DataChannelEvent event, dynamic data) {
      FileChunk chunk = data;
      _chunks.putIfAbsent(chunk.identifier, () => chunk);
    };
  }

  Future<void> receiveData(TextEditingController textController) async {
    bool allChannelsInitialized = false;
    bool fileMetadataReceived = false;

    if (_channels.isEmpty) {
      throw StateError('Cannot receive file because receiver has no channel.');
    }

    int initializedChannels = 0;

    // Open bootstrap channel.
    bootstrapChannel.on = (BootstrapChannelEvent event, dynamic data) async {
      switch(event) {
        case BootstrapChannelEvent.fileMetadata:
          debugPrint("*********** Received BootstrapChannelEvent.fileMetadata");
          FileMetadata fileMetadata = data;
          _filename = fileMetadata.name;
          _chunksCount = fileMetadata.chunkCount;
          fileMetadataReceived = true;
          break;
        case BootstrapChannelEvent.channelMetadata:
          debugPrint("*********** Received BootstrapChannelEvent.channelMetadata");
          ChannelMetadata channelMetadata = data;

          // Get matching channel to only send data to it, and not other channels.
          DataChannel matchingChannel = _channels.firstWhere((element) =>
              element.identifier == channelMetadata.channelIdentifier,
              orElse: () => throw ArgumentError(
                  'No channel with identifier "${channelMetadata.channelIdentifier}" was found in receiver channels.')
          );
          await matchingChannel.initReceiver(channelMetadata);

          // Start receiving once all channels have been initialized.
          initializedChannels += 1;
          if (initializedChannels == _channels.length) {
            allChannelsInitialized = true;
          }

          break;
      }
    };

    await bootstrapChannel.initReceiver();

    // Wait for bootstrap channel to receive channel information and initialize
    // them.
    while (!allChannelsInitialized || !fileMetadataReceived) {
      await Future.delayed(const Duration(milliseconds: 100));
    }

    // Wait for all chunks to arrive.
    await receiveAllChunks();


    for (var chunk in _chunks.values) {
      textController.text = textController.text + String.fromCharCodes(chunk.data);
    }
  }


  Future<void> receiveAllChunks() async {
    while (_chunks.length != _chunksCount) {
      await Future.delayed(const Duration(milliseconds: 200));
    }
  }
}