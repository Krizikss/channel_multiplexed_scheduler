import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:async/async.dart';
import 'package:venice_core/channels/abstractions/bootstrap_channel.dart';
import 'package:venice_core/channels/events/data_channel_event.dart';
import 'package:venice_core/channels/abstractions/data_channel.dart';
import 'package:venice_core/file/file_chunk.dart';
import 'package:venice_core/file/file_metadata.dart';
import 'package:flutter/material.dart';


/// The Scheduler class goal is to send a file to a Receiver instance that's
/// located on another device.
///
/// To do so, it uses multiple data channels; it is responsible for their
/// initialization, but also in the implementation of the channel choice
/// strategy (*i.e.* in choosing which channel to use to send a given file
/// chunk).
abstract class Scheduler {
  final BootstrapChannel bootstrapChannel;
  late final List<DataChannel> _channels = [];
  late List<FileChunk> _chunksQueue = [];
  final Map<int, CancelableOperation> _resubmissionTimers = {};

  Scheduler(this.bootstrapChannel);


  /// Adds a channel to be used to send file chunks.
  void useChannel(DataChannel channel) {
    if (_channels.where((element) => element.identifier == channel.identifier).isNotEmpty) {
      throw ArgumentError('Channel identifier "${channel.identifier}" is already used.');
    }
    
    _channels.add(channel);
    channel.on = (DataChannelEvent event, dynamic data) {
      switch (event) {
        case DataChannelEvent.acknowledgment:
          int chunkId = data;
          if (_resubmissionTimers.containsKey(chunkId)) {
            CancelableOperation timer = _resubmissionTimers.remove(chunkId)!;
            timer.cancel();
          }
          break;
        case DataChannelEvent.data:
          break;
      }
    };
  }


  Future<void> sendData(String data, int chunksize) async {
    if (_channels.isEmpty) {
      throw StateError('Cannot send data because scheduler has no channel.');
    }

    _chunksQueue = splitData(data, chunksize);

    // Open bootstrap channel and send file metadata.
    await bootstrapChannel.initSender();
    await bootstrapChannel.sendFileMetadata(
        FileMetadata("data", chunksize, _chunksQueue.length)
    );

    // Open all channels.
    await Future.wait(_channels.map((c) => c.initSender( bootstrapChannel )));
    debugPrint("[Scheduler] All data channels are ready, data sending can start.\n");

    // Begin sending chunks.
    await sendChunks(_chunksQueue, _channels, _resubmissionTimers);
  }

  /// This lets Scheduler instances implement their own chunks sending policy.
  /// 
  /// The implementation should send all chunks' content, by calling the 
  /// sendChunk method; it can also check for any resubmission timer presence, 
  /// to avoid finishing execution while some chunks have not been acknowledged.
  Future<void> sendChunks(
      List<FileChunk> chunks,
      List<DataChannel> channels,
      Map<int, CancelableOperation> resubmissionTimers);

  /// Sends a data chunk through a specified channel.
  /// 
  /// If such chunk is not acknowledged within a given duration, this will put
  /// the chunk at the head of the sending queue, for it to be resent as soon
  /// as possible.
  Future<void> sendChunk(FileChunk chunk, DataChannel channel) async {
    bool acknowledged = false;
    bool timedOut = false;

    _resubmissionTimers.putIfAbsent(
        chunk.identifier,
            () => CancelableOperation.fromFuture(
            Future.delayed(const Duration(seconds: 1), () {
              // Do not trigger chunk resending if it was previously
              // acknowledged.
              if (acknowledged) return;
              debugPrint("[Scheduler] Chunk n°${chunk.identifier} was not acknowledged in time, resending.");
              CancelableOperation timer = _resubmissionTimers.remove(chunk.identifier)!;
              timedOut = true;
              timer.cancel();
              _chunksQueue.insert(0, chunk);
            }),
              onCancel: () {
                // Do not print message if onCancel was called due to request
                // timeout.
                if (timedOut) return;
                acknowledged = true;
                debugPrint('[Scheduler] Chunk n°${chunk.identifier} was acknowledged.');
              }
        )
    );

    debugPrint("[Scheduler] Sending chunk n°${chunk.identifier}.");
    await channel.sendChunk(chunk);
  }

  List<FileChunk> splitData (String data, int chunksize) {

    Uint8List bytes = Uint8List.fromList(data.codeUnits);
    List<FileChunk> chunks = [];
    int bytesCount = bytes.length;
    int index = 0;

    if (chunksize <= 0 || chunksize > bytesCount) {
      throw RangeError('Invalid chunk size (was $chunksize).');
    }

    for (int i=0; i<bytesCount; i += chunksize) {
      chunks.add(FileChunk(
          identifier: index,
          data: bytes.sublist(i, i + chunksize > bytesCount
              ? bytesCount
              : i + chunksize)
      ));
      index += 1;
    }

    return chunks;
  }
}