import 'dart:io';

import 'package:channel_multiplexed_scheduler/channels/channel_metadata.dart';
import 'package:channel_multiplexed_scheduler/channels/events/bootstrap_channel_event.dart';
import 'package:channel_multiplexed_scheduler/channels/abstractions/bootstrap_channel.dart';
import 'package:channel_multiplexed_scheduler/channels/events/data_channel_event.dart';
import 'package:channel_multiplexed_scheduler/channels/abstractions/data_channel.dart';
import 'package:channel_multiplexed_scheduler/file/file_chunk.dart';
import 'package:channel_multiplexed_scheduler/file/file_metadata.dart';


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
      throw StateError('Channel identifier "${channel.identifier}" is already used.');
    }
    
    _channels.add(channel);
    channel.on = (DataChannelEvent event, dynamic data) {
      FileChunk chunk = data;
      _chunks.putIfAbsent(chunk.identifier, () => chunk);
    };
  }

  /// Receives a file through available channels.
  Future<void> receiveFile(Directory destination) async {
    bool allChannelsInitialized = false;
    bool fileMetadataReceived = false;

    if (_channels.isEmpty) {
      throw StateError('Cannot receive file because receiver has no channel.');
    }

    if (!destination.existsSync()) {
      throw ArgumentError('Destination directory does not exist.');
    }

    // Open bootstrap channel.
    bootstrapChannel.on = (BootstrapChannelEvent event, dynamic data) async {
      switch(event) {
        case BootstrapChannelEvent.fileMetadata:
          // TODO use chunk size
          FileMetadata fileMetadata = data;
          _filename = fileMetadata.name;
          _chunksCount = fileMetadata.chunkCount;
          fileMetadataReceived = true;
          break;
        case BootstrapChannelEvent.channelMetadata:
          // Open all channels.
          ChannelMetadata channelMetadata = data;
          // TODO add a channel identifier, not to send all metadata to all channels
          // TODO set allChannelsInitialized to true once all channels have been initialized
          // TODO throw if corresponding channel end was not found
          await Future.wait(_channels.map((c) => c.initReceiver( channelMetadata )));
          allChannelsInitialized = true;
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

    // Fill destination file with received chunks.
    File newFile = File(destination.path+Platform.pathSeparator+_filename);
    if (newFile.existsSync()) {
      newFile.deleteSync();
    }
    newFile.createSync();
    for (var chunk in _chunks.values) {
      newFile.writeAsBytesSync(chunk.data, mode: FileMode.append);
    }
  }


  Future<void> receiveAllChunks() async {
    while (_chunks.length != _chunksCount) {
      await Future.delayed(const Duration(milliseconds: 200));
    }
  }
}